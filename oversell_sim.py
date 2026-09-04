"""
Concurrency simulation for Advanced Task 01 post-mortem paper.
Models a single hot SKU (RTX 3080 Founders Edition) with limited stock,
and a swarm of concurrent "buy" transactions hitting it.

Two isolation modes:
  - UNSAFE  : no lock around the Read -> synthetic delay -> Write sequence
              (mirrors the retailer race condition that produced overselling)
  - SAFE    : a per-SKU mutex wraps the entire transaction (mirrors strict
              2PL / row-level locking)

For each thread count in THREAD_LEVELS we reset stock to INITIAL_STOCK and
run both modes, recording:
  - final_stock            (can go negative in UNSAFE mode -> direct proof of overselling)
  - orders_committed       (rows written to the Order/OrderItem tables)
  - oversold_units         (orders_committed - INITIAL_STOCK, floor 0)
  - throughput_tps         (successful transactions / wall-clock seconds)
  - avg_latency_ms / p95_latency_ms
  - timeouts                (transactions whose end-to-end latency exceeded
                              TIMEOUT_THRESHOLD_S, used as a bottleneck proxy
                              for the strict-isolation "queuing" phase)
"""

import threading
import time
import statistics
import json
from pathlib import Path

INITIAL_STOCK = 50
PROCESS_DELAY_S = 0.005          # simulated DB write / network latency per transaction
TIMEOUT_THRESHOLD_S = 0.25       # a thread waiting longer than this is logged as "starved"
THREAD_LEVELS = [10, 100, 1000]


class InventoryRecord:
    """Represents one row of the INVENTORY entity for a single SKU."""
    def __init__(self, sku, quantity):
        self.sku = sku
        self.quantity = quantity


class Product:
    def __init__(self, sku, name, price):
        self.sku = sku
        self.name = name
        self.price = price


class Order:
    _next_id = 1
    _id_lock = threading.Lock()

    def __init__(self, customer_id, sku):
        with Order._id_lock:
            self.order_id = Order._next_id
            Order._next_id += 1
        self.customer_id = customer_id
        self.sku = sku
        self.status = "COMMITTED"


def do_transaction(inv, orders, customer_id, use_lock, lock, latencies, timeouts_flag):
    """
    Simulated multi-step transaction against the conceptual data model:
      1. READ  : check INVENTORY.quantity for the SKU
      2. PROCESS: synthetic delay (server processing / network latency)
      3. WRITE : decrement INVENTORY.quantity, insert ORDER + ORDER_ITEM
    """
    t_start = time.perf_counter()

    def critical_section():
        qty = inv.quantity                    # READ
        time.sleep(PROCESS_DELAY_S)            # PROCESS (synthetic commit delay)
        if qty > 0:                            # (re-check happens on stale value in UNSAFE mode)
            inv.quantity = qty - 1             # WRITE
            orders.append(Order(customer_id, inv.sku))

    if use_lock:
        acquired_at = time.perf_counter()
        with lock:
            critical_section()
    else:
        critical_section()

    t_end = time.perf_counter()
    latency = t_end - t_start
    latencies.append(latency)
    if latency > TIMEOUT_THRESHOLD_S:
        timeouts_flag.append(1)


def run_phase(n_threads, use_lock):
    # Instantiate the Product entity represented in the ERD.
    product = Product("RTX3080-FE", "RTX 3080 Founders Edition", 699.99)
    inv = InventoryRecord(product.sku, INITIAL_STOCK)
    orders = []
    latencies = []
    timeouts_flag = []
    lock = threading.Lock()

    threads = []
    wall_start = time.perf_counter()
    for i in range(n_threads):
        t = threading.Thread(
            target=do_transaction,
            args=(inv, orders, f"cust_{i}", use_lock, lock, latencies, timeouts_flag),
        )
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()
    wall_end = time.perf_counter()

    wall_time = wall_end - wall_start
    committed = len(orders)
    oversold = max(0, committed - INITIAL_STOCK)
    throughput = committed / wall_time if wall_time > 0 else 0.0
    avg_latency_ms = statistics.mean(latencies) * 1000 if latencies else 0.0
    p95_latency_ms = (
        statistics.quantiles(latencies, n=100)[94] * 1000
        if len(latencies) >= 20 else max(latencies) * 1000 if latencies else 0.0
    )
    return {
        "n_threads": n_threads,
        "mode": "SAFE (locked)" if use_lock else "UNSAFE (unlocked)",
        "final_stock": inv.quantity,
        "orders_committed": committed,
        "oversold_units": oversold,
        "wall_time_s": round(wall_time, 4),
        "throughput_tps": round(throughput, 2),
        "avg_latency_ms": round(avg_latency_ms, 3),
        "p95_latency_ms": round(p95_latency_ms, 3),
        "timeouts_gt_250ms": len(timeouts_flag),
    }


def main():
    Order._next_id = 1
    results = []
    for n in THREAD_LEVELS:
        for use_lock in (False, True):
            r = run_phase(n, use_lock)
            results.append(r)
            print(json.dumps(r))
    # Write beside the script so it works on any computer.
    output_path = Path(__file__).with_name("results.json")
    with output_path.open("w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {output_path}")


if __name__ == "__main__":
    main()
