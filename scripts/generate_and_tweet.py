# scripts/generate_and_tweet.py
import os
import tempfile
import pandas as pd

from generate_content_assets import main as generate_assets
from x_publisher import post_tweet
from release_data_loader import load_release_snapshots, get_latest_snapshot


OUTPUT_DIR = "outputs"
CONTENT_TYPE = os.environ.get("CONTENT_TYPE", "avg")  # avg | up | down
HEADLESS = os.environ.get("HEADLESS", "true").lower() == "true"

# Guardarraíl
MAX_DATA_AGE_DAYS = int(os.environ.get("MAX_DATA_AGE_DAYS", "1"))
ALLOW_STALE = os.environ.get("ALLOW_STALE", "false").lower() == "true"
CCAA = os.environ.get("CCAA", "madrid")


def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def resolve_assets(content_type: str) -> tuple[str, str]:
    mapping = {
        "avg": ("tweet_avg_price.txt", "tweet_avg_price.png"),
        "up": ("tweet_top_up.txt", "tweet_top_up.png"),
        "down": ("tweet_top_down.txt", "tweet_top_down.png"),
    }

    if content_type not in mapping:
        raise ValueError(f"CONTENT_TYPE inválido: {content_type}")

    txt_name, img_name = mapping[content_type]
    return (
        os.path.join(OUTPUT_DIR, txt_name),
        os.path.join(OUTPUT_DIR, img_name),
    )


def assert_fresh_release_data(ccaa: str, max_age_days: int, allow_stale: bool) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        snapshots = load_release_snapshots(ccaa, tmpdir)

        if not snapshots:
            raise RuntimeError(f"No se han encontrado snapshots para {ccaa}")

        latest_snap = get_latest_snapshot(snapshots)
        latest_dt = pd.to_datetime(latest_snap.date_str).date()
        today_utc = pd.Timestamp.utcnow().date()

        age_days = (today_utc - latest_dt).days

        print(f"ℹ️ Último snapshot detectado para {ccaa}: {latest_dt} ({age_days} días de antigüedad)")

        if age_days > max_age_days:
            msg = (
                f"❌ Datos demasiado antiguos para publicar en X. "
                f"Último snapshot: {latest_dt} | hoy UTC: {today_utc} | "
                f"antigüedad: {age_days} días | máximo permitido: {max_age_days}"
            )

            if allow_stale:
                print("⚠️ ALLOW_STALE=true, se continúa pese a histórico desactualizado.")
                print(msg)
                return

            raise RuntimeError(msg)


def main():
    # 1) Guardarraíl ANTES de generar/publicar
    assert_fresh_release_data(
        ccaa=CCAA,
        max_age_days=MAX_DATA_AGE_DAYS,
        allow_stale=ALLOW_STALE
    )

    # 2) Generar assets
    generate_assets()

    # 3) Resolver assets del tipo de contenido
    txt_path, img_path = resolve_assets(CONTENT_TYPE)
    tweet_text = read_text(txt_path)

    # 4) Publicar
    post_tweet(tweet_text, media_paths=[img_path], headless=HEADLESS)

    print("✅ Tweet publicado")
    print(f"Tipo: {CONTENT_TYPE}")
    print(tweet_text)


if __name__ == "__main__":
    main()