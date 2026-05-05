#!/usr/bin/env python3
"""
Сканер pending-транзакций из мемпула (Ethereum и EVM-совместимые сети).

Нужен WebSocket RPC с поддержкой eth_subscribe / newPendingTransactions.
Фильтры: нативная сумма перевода (поле value), адрес получателя to (роутеры DEX).

Важно: большинство свапов на DEX идёт с value=0 (оплата только газом); такие сделки
не попадут в фильтр по сумме в ETH. Фильтр по to=роутер как раз для «заявок» на DEX.

Примеры:
  export WEB3_WS_URI=wss://ethereum.publicnode.com
  python3 pending_mempool.py --dex uniswap_v2,sushiswap --min-eth 0 --limit 5

  python3 pending_mempool.py --to 0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D --max-eth 10

Переменные окружения:
  WEB3_WS_URI — WebSocket endpoint (можно вместо --ws-uri).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from decimal import Decimal
from typing import Any

from eth_utils import to_checksum_address
from web3 import AsyncWeb3
from web3.providers import WebSocketProvider

# Распространённые роутеры Ethereum mainnet (ключи для --dex).
DEX_ROUTERS_MAINNET: dict[str, str] = {
    "uniswap_v2": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
    "uniswap_v3": "0xE592427A0AEce92De3Edee1F18E0157C05861564",
    "uniswap_v3_02": "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
    "sushiswap": "0xd9e1cE17f2641f24aE83637ab66a521ccaC0e732",
    "oneinch_v5": "0x1111111254EEB25477B68fb85Ed929f73A960582",
    "oneinch_v6": "0x111111125421ca6dc452d289314280a0f8842a65",
    "curve_router": "0x99a58482BD75cbab83b27EC03CA68fF480bDCD36",
    "balancer_vault": "0xBA12222222228d8Ba445958a75a0704d566BF2C8",
}


def _wei_from_ether(s: str) -> int:
    d = Decimal(s)
    return int(d * Decimal(10**18))


def _format_tx_summary(tx: dict[str, Any]) -> str:
    h = tx.get("hash")
    if hasattr(h, "hex"):
        h = h.hex()
    frm = tx.get("from", "")
    if hasattr(frm, "lower"):
        frm = str(frm).lower()
    to = tx.get("to")
    if to is None:
        to_s = "contract_create"
    elif hasattr(to, "lower"):
        to_s = str(to).lower()
    else:
        to_s = str(to)
    val = int(tx.get("value", 0))
    val_eth = Decimal(val) / Decimal(10**18)
    gas = tx.get("gas")
    gas_price = tx.get("gasPrice") or tx.get("maxFeePerGas")
    gp = int(gas_price) if gas_price is not None else None
    return (
        f"hash={h} from={frm} to={to_s} "
        f"value={val_eth:.6f} ETH ({val} wei) gas={gas} gasPrice={gp}"
    )


def _parse_dex_list(spec: str | None) -> list[str]:
    if not spec or not spec.strip():
        return []
    names = [x.strip().lower() for x in spec.split(",") if x.strip()]
    addrs: list[str] = []
    for n in names:
        if n not in DEX_ROUTERS_MAINNET:
            print(
                f"Неизвестный ключ DEX: {n}. Доступные: {', '.join(sorted(DEX_ROUTERS_MAINNET))}",
                file=sys.stderr,
            )
            sys.exit(2)
        addrs.append(to_checksum_address(DEX_ROUTERS_MAINNET[n]))
    return addrs


def _parse_to_addresses(spec: str | None) -> list[str]:
    if not spec or not spec.strip():
        return []
    out: list[str] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        out.append(to_checksum_address(part))
    return out


def _tx_to_jsonable(tx: Any) -> dict[str, Any]:
    """Привести AttributeDict / HexBytes к обычным типам для json.dumps."""
    if hasattr(tx, "items"):
        d = dict(tx.items())
    else:
        d = dict(tx)
    out: dict[str, Any] = {}
    for k, v in d.items():
        if hasattr(v, "hex") and callable(getattr(v, "hex")):
            try:
                out[k] = v.hex()
                continue
            except Exception:
                pass
        if isinstance(v, bytes):
            out[k] = "0x" + v.hex()
        else:
            out[k] = v
    return out


async def _run(
    ws_uri: str,
    min_wei: int,
    max_wei: int | None,
    allowed_to: set[str] | None,
    json_out: bool,
    limit: int | None,
) -> None:
    provider = WebSocketProvider(ws_uri)
    async with AsyncWeb3(provider) as w3:
        try:
            sub_id = await w3.eth.subscribe("newPendingTransactions", True)
        except Exception as e:
            print(
                "Не удалось подписаться на newPendingTransactions (полные транзакции). "
                "Проверьте WebSocket URI и что нода поддерживает подписку.\n"
                f"Ошибка: {e}",
                file=sys.stderr,
            )
            sys.exit(1)

        seen = 0
        try:
            async for msg in w3.socket.process_subscriptions():
                if limit is not None and seen >= limit:
                    break
                if msg.get("subscription") != sub_id:
                    continue
                tx = msg.get("result")
                if tx is None or not hasattr(tx, "get"):
                    continue

                value = int(tx.get("value", 0))
                if value < min_wei:
                    continue
                if max_wei is not None and value > max_wei:
                    continue

                to_addr = tx.get("to")
                if allowed_to is not None:
                    if to_addr is None:
                        continue
                    t = to_checksum_address(to_addr)
                    if t not in allowed_to:
                        continue

                seen += 1
                if json_out:
                    print(json.dumps(_tx_to_jsonable(tx), default=str))
                else:
                    print(_format_tx_summary(tx))
                sys.stdout.flush()
        finally:
            try:
                await w3.eth.unsubscribe(sub_id)
            except Exception:
                pass


def main() -> None:
    p = argparse.ArgumentParser(
        description="Pending-транзакции из мемпула: фильтры по value (ETH) и to (DEX / адреса)."
    )
    p.add_argument(
        "--ws-uri",
        default=os.environ.get("WEB3_WS_URI", ""),
        help="WebSocket RPC или переменная WEB3_WS_URI",
    )
    p.add_argument(
        "--dex",
        default="",
        help="Ключи через запятую: uniswap_v2,sushiswap,...",
    )
    p.add_argument(
        "--to",
        dest="to_addrs",
        default="",
        help="Дополнительные адреса to через запятую",
    )
    p.add_argument("--min-eth", default="0", help="Минимум value в ETH")
    p.add_argument("--max-eth", default="", help="Максимум value в ETH; пусто = нет потолка")
    p.add_argument("--min-wei", default="", help="Минимум value в wei (перекрывает --min-eth)")
    p.add_argument("--max-wei", default="", help="Максимум value в wei (перекрывает --max-eth)")
    p.add_argument("--json", action="store_true", help="Вывод полной транзакции в JSON")
    p.add_argument("--limit", type=int, default=0, help="Остановиться после N совпадений (0 = без лимита)")
    p.add_argument("--list-dex", action="store_true", help="Показать известные DEX-ключи и выйти")
    args = p.parse_args()

    if args.list_dex:
        for k, v in sorted(DEX_ROUTERS_MAINNET.items()):
            print(f"{k}: {v}")
        return

    ws_uri = (args.ws_uri or "").strip()
    if not ws_uri:
        print("Укажите WebSocket: --ws-uri или переменную WEB3_WS_URI", file=sys.stderr)
        sys.exit(2)

    min_wei = int(args.min_wei) if str(args.min_wei).strip() else _wei_from_ether(args.min_eth)
    max_wei: int | None
    if str(args.max_wei).strip():
        max_wei = int(args.max_wei)
    elif str(args.max_eth).strip():
        max_wei = _wei_from_ether(args.max_eth)
    else:
        max_wei = None

    dex_addrs = _parse_dex_list(args.dex or None)
    explicit = _parse_to_addresses(args.to_addrs or None)
    combined = dex_addrs + explicit
    allowed: set[str] | None = None
    if combined:
        allowed = {to_checksum_address(a) for a in combined}

    limit = args.limit if args.limit > 0 else None

    asyncio.run(
        _run(
            ws_uri=ws_uri,
            min_wei=min_wei,
            max_wei=max_wei,
            allowed_to=allowed,
            json_out=args.json,
            limit=limit,
        )
    )


if __name__ == "__main__":
    main()
