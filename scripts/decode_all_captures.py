import subprocess
import sys
from pathlib import Path


def decode_file(file_path: Path) -> int:
    output_path = file_path.with_suffix(".decoded.txt")

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        "--decode_raw",
    ]

    with file_path.open("rb") as f:
        result = subprocess.run(
            cmd,
            stdin=f,
            capture_output=True,
            text=True,
            check=False,
        )

    if result.returncode != 0:
        output_path.write_text(
            "Decode failed:\n\n" + result.stderr,
            encoding="utf-8",
        )
        print(f"FAILED: {file_path.name}")
        return result.returncode

    output_path.write_text(result.stdout, encoding="utf-8")
    print(f"Decoded: {file_path.name} -> {output_path.name}")
    return 0


def main() -> None:
    captures_dir = Path("captures")

    if not captures_dir.exists():
        print("captures folder not found")
        raise SystemExit(1)

    bin_files = sorted(captures_dir.glob("*.bin"))

    if not bin_files:
        print("No .bin files found in captures")
        return

    failures = 0

    for file_path in bin_files:
        rc = decode_file(file_path)
        if rc != 0:
            failures += 1

    print()
    print(f"Done. Decoded {len(bin_files) - failures} of {len(bin_files)} files.")
    if failures:
        print(f"Failures: {failures}")


if __name__ == "__main__":
    main()