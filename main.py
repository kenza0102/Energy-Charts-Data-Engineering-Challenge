from src.pipeline import run

if __name__ == "__main__":
    # local execution (last 3 days)
    run(start="2026-01-10", end="2026-01-12", country="de", zone="DE-LU")