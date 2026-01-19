import os
import stat
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from testcontainers.core.container import DockerContainer

ENV_SQLC_IMAGE = "IRON_SQL_SQLC_IMAGE"
ENV_SQLC_MOUNT = "IRON_SQL_SQLC_MOUNT"
ENV_SQLC_TIMEOUT_SECONDS = "IRON_SQL_SQLC_TIMEOUT_SECONDS"
ENV_SQLC_ADD_HOST_GATEWAY = "IRON_SQL_SQLC_ADD_HOST_GATEWAY"

DEFAULT_SQLC_IMAGE = "sqlc/sqlc:1.29.0"
DEFAULT_TIMEOUT_SECONDS = 120


@dataclass(frozen=True, slots=True)
class SqlcRunResult:
    exit_code: int
    stdout: bytes
    stderr: bytes


@contextmanager
def _temporary_env(values: dict[str, str]) -> Iterator[None]:
    old: dict[str, str | None] = {k: os.environ.get(k) for k in values}
    try:
        os.environ.update(values)
        yield
    finally:
        for k, prev in old.items():
            if prev is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = prev


def _is_subpath(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def run_sqlc_in_container(
    *, argv: list[str], mount: Path, image: str, timeout_s: int
) -> SqlcRunResult:
    mount = mount.resolve()

    workdir = Path.cwd().resolve()
    if not _is_subpath(workdir, mount):
        # Если вызывают из cwd вне маунта — не ломаемся, просто уходим в mount.
        workdir = mount

    container = (
        DockerContainer(image)
        .with_volume_mapping(str(mount), str(mount), mode="rw")
        .with_kwargs(working_dir=str(workdir))
        .with_command(argv)
    )

    add_host_gateway = os.environ.get(ENV_SQLC_ADD_HOST_GATEWAY, "1") != "0"
    if add_host_gateway:
        # Это эквивалентно: --add-host=localhost:host-gateway
        container = container.with_kwargs(extra_hosts={"localhost": "host-gateway"})

    container.start()
    try:
        wrapped = container.get_wrapped_container()

        wait_res = wrapped.wait(timeout=timeout_s)
        exit_code = int(wait_res.get("StatusCode", 1))

        # docker-py позволяет просить stdout/stderr отдельно
        stdout = wrapped.logs(stdout=True, stderr=False) or b""
        stderr = wrapped.logs(stdout=False, stderr=True) or b""

        return SqlcRunResult(exit_code=exit_code, stdout=stdout, stderr=stderr)
    finally:
        container.stop()


def shim_main() -> int:
    mount_raw = os.environ.get(ENV_SQLC_MOUNT)
    if not mount_raw:
        msg = f"{ENV_SQLC_MOUNT} is required"
        raise RuntimeError(msg)

    image = os.environ.get(ENV_SQLC_IMAGE, DEFAULT_SQLC_IMAGE)
    timeout_s = int(
        os.environ.get(ENV_SQLC_TIMEOUT_SECONDS, str(DEFAULT_TIMEOUT_SECONDS))
    )

    res = run_sqlc_in_container(
        argv=sys.argv[1:],
        mount=Path(mount_raw),
        image=image,
        timeout_s=timeout_s,
    )

    # Важно: generator читает stderr при ошибке.
    if res.stdout:
        sys.stdout.buffer.write(res.stdout)
    if res.stderr:
        sys.stderr.buffer.write(res.stderr)

    return res.exit_code


@dataclass(frozen=True, slots=True)
class SqlcShim:
    path: Path
    image: str = DEFAULT_SQLC_IMAGE

    def __init__(self, script_dir: Path, *, image: str = DEFAULT_SQLC_IMAGE) -> None:
        script_dir = script_dir.resolve()
        script_dir.mkdir(parents=True, exist_ok=True)

        shim_path = script_dir / "sqlc"
        shim_path.write_text(
            "#!/usr/bin/env python3\n"
            "from iron_sql.testing.sqlc_testcontainers import shim_main\n"
            "raise SystemExit(shim_main())\n",
            encoding="utf-8",
        )
        shim_path.chmod(
            shim_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
        )

        object.__setattr__(self, "path", shim_path)
        object.__setattr__(self, "image", image)

    @contextmanager
    def env_context(self, mount_path: Path) -> Iterator[None]:
        mount = str(mount_path.resolve())
        # TMPDIR is important:
        # run_sqlc uses TemporaryDirectory and symlink_to(absolute_path),
        # and we need the tempdir to reside inside the mount.
        with _temporary_env({
            "TMPDIR": mount,
            ENV_SQLC_MOUNT: mount,
            ENV_SQLC_IMAGE: self.image,
        }):
            yield
