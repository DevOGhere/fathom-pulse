"""python -m pulse.api  ->  serves http://127.0.0.1:8765 (host-side, never containerised: S2)."""
import uvicorn

if __name__ == "__main__":
    uvicorn.run("pulse.api.app:app", host="127.0.0.1", port=8765, log_level="warning")
