"""Single CLI entry: web server (default) or ``pipeline`` subcommand."""
from __future__ import annotations

import os
import sys


def main() -> None:
    from app.config.settings import load_dotenv_from_repo

    load_dotenv_from_repo()

    if len(sys.argv) > 1 and sys.argv[1] == "pipeline":
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        from app.pipeline import main as pipeline_main

        pipeline_main()
        return

    port = int(os.environ.get("ARMOR_PORT", "8765"))
    from app import create_app

    flask_app = create_app()
    print("Armor Data Anonymizer — http://127.0.0.1:{}/".format(port))
    gliner_env = os.environ.get("ARMOR_GLINER_MODEL")
    if gliner_env:
        print("GLiNER model (ARMOR_GLINER_MODEL):", gliner_env)
    else:
        print(
            "GLiNER: knowledgator/gliner-x-large (if OOM: "
            "ARMOR_GLINER_MODEL=urchade/gliner_medium-v2.1 python main.py)",
        )
    flask_app.run(host="0.0.0.0", port=port, threaded=True, debug=flask_app.debug)


if __name__ == "__main__":
    main()
