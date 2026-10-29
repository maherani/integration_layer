# -*- coding: utf-8 -*-
"""Main runner for the standalone Integration Layer."""

import argparse
import logging
import signal
import sys
import threading

from config import default_config
from file_proxy import create_server
from publisher import OutboxPublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("IntegrationLayer.Main")

def run():
    parser = argparse.ArgumentParser(description="Standalone Integration Layer (Archive <-> AI Box)")
    parser.add_argument(
        "--mode",
        choices=["all", "proxy", "publisher"],
        default="all",
        help="Subsystem to run: 'all', 'proxy' (API proxy), or 'publisher' (Outbox relay)",
    )
    args = parser.parse_args()

    stop_event = threading.Event()

    def handle_signal(sig, frame):
        logger.info("Termination signal received. Shutting down...")
        stop_event.set()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    threads = []

    if args.mode in ("all", "proxy"):
        server = create_server(default_config)
        logger.info(
            "Starting Streaming File Proxy on %s:%d (auth_token=%s...)",
            default_config.proxy_host,
            default_config.proxy_port,
            default_config.proxy_auth_token[:6],
        )
        proxy_thread = threading.Thread(target=server.serve_forever, daemon=True, name="ProxyServerThread")
        proxy_thread.start()
        threads.append(proxy_thread)

    if args.mode in ("all", "publisher"):
        publisher = OutboxPublisher(default_config)
        logger.info(
            "Starting Outbox Publisher relaying from Archive (%s) to RabbitMQ (%s:%d)",
            default_config.archive_base_url,
            default_config.rabbitmq_host,
            default_config.rabbitmq_port,
        )
        pub_thread = threading.Thread(target=publisher.run_loop, args=(stop_event,), daemon=True, name="PublisherLoopThread")
        pub_thread.start()
        threads.append(pub_thread)

    for t in threads:
        t.join()

if __name__ == "__main__":
    run()
