import io
import tarfile
from dataclasses import dataclass
from pathlib import Path

import docker
from testcontainers.core.container import DockerContainer

DEFAULT_SQLC_IMAGE = "sqlc/sqlc:1.30.0"
DEFAULT_HELPER_IMAGE = "alpine:3.20"
DEFAULT_TIMEOUT_SECONDS = 120


def _fetch_sqlc_binary(image: str) -> bytes:
    client = docker.from_env()
    if not client.images.list(filters={"reference": image}):
        client.images.pull(image)
    container = client.containers.create(image)
    try:
        stream, _ = container.get_archive("/workspace/sqlc")
        data = b"".join(stream)
    finally:
        container.remove()
        client.close()

    with tarfile.open(fileobj=io.BytesIO(data)) as tar:
        member = next(
            (m for m in tar.getmembers() if Path(m.name).name == "sqlc"),
            None,
        )
        if member is None:
            msg = "sqlc binary not found in archive"
            raise RuntimeError(msg)
        fileobj = tar.extractfile(member)
        if fileobj is None:
            msg = "sqlc binary could not be extracted"
            raise RuntimeError(msg)
        return fileobj.read()


def _build_sqlc_archive(sqlc_binary: bytes) -> bytes:
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w") as tar:
        info = tarfile.TarInfo(name="sqlc")
        info.size = len(sqlc_binary)
        info.mode = 0o755
        tar.addfile(info, io.BytesIO(sqlc_binary))
    return out.getvalue()


@dataclass(slots=True)
class SqlcContainer:
    image: str = DEFAULT_SQLC_IMAGE
    helper_image: str = DEFAULT_HELPER_IMAGE
    timeout_s: int = DEFAULT_TIMEOUT_SECONDS
    add_host_gateway: bool = True
    _container_id: str | None = None
    _mount: Path | None = None
    _container: DockerContainer | None = None

    def start(self, mount: Path) -> None:
        if self._container is not None:
            msg = "SqlcContainer already started"
            raise RuntimeError(msg)

        mount = mount.resolve()
        sqlc_archive = _build_sqlc_archive(_fetch_sqlc_binary(self.image))
        container = DockerContainer(self.helper_image).with_volume_mapping(
            str(mount),
            str(mount),
            mode="rw",
        )
        container = container.with_kwargs(
            working_dir=str(mount),
        ).with_command(["/bin/sh", "-c", "sleep infinity"])
        if self.add_host_gateway:
            container = container.with_kwargs(extra_hosts={"localhost": "host-gateway"})

        container.start()
        wrapped = container.get_wrapped_container()
        if not wrapped.put_archive("/usr/local/bin", sqlc_archive):
            container.stop()
            msg = "failed to install sqlc in helper container"
            raise RuntimeError(msg)

        self._container = container
        self._container_id = wrapped.id
        self._mount = mount

    def stop(self) -> None:
        if self._container is None:
            return
        self._container.stop()
        self._container = None
        self._container_id = None
        self._mount = None

    def sqlc_command(self) -> list[str]:
        if self._container_id is None or self._mount is None:
            msg = "SqlcContainer is not started"
            raise RuntimeError(msg)

        return ["docker", "exec", "-w", str(self._mount), self._container_id, "sqlc"]
