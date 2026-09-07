"""Small ACID concurrency demo for Advanced Task 01."""
import json, time, threading
from concurrent.futures import ThreadPoolExecutor

STOCK, DELAY, TIMEOUT = 50, .005, .25

product = {"sku": "RTX3080-FE", "name": "RTX 3080 Founders Edition"}

def run(n, locked):
    inventory = {"sku": product["sku"], "quantity": STOCK}
    orders, times, timeouts, mutex = [], [], [], threading.Lock()

    def buy(customer):
        start = time.perf_counter()

        def transaction():
            # READ -> PROCESS -> WRITE
            quantity = inventory["quantity"]
            time.sleep(DELAY)
            if quantity > 0:
                inventory["quantity"] = quantity - 1
                orders.append({"customer": customer, "sku": product["sku"],
                               "status": "COMMITTED"})

        if locked:
            with mutex:
                transaction()
        else:
            transaction()

        elapsed = time.perf_counter() - start
        times.append(elapsed)
        if elapsed > TIMEOUT:
            timeouts.append(1)

    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(buy, range(n)))
    wall = time.perf_counter() - start

    return {
        "threads": n,
        "mode": "strict isolation" if locked else "unenforced isolation",
        "final_inventory": inventory["quantity"],
        "orders_committed": len(orders),
        "oversold_units": max(0, len(orders) - STOCK),
        "throughput_tps": round(len(orders) / wall, 2),
        "avg_latency_ms": round(sum(times) / len(times) * 1000, 2),
        "timeouts_over_250ms": len(timeouts),
    }

results = [run(n, locked) for n in (10, 100, 1000) for locked in (False, True)]
print(*[json.dumps(row) for row in results], sep="\n")
with open("results.json", "w") as file:
    json.dump(results, file, indent=2)
