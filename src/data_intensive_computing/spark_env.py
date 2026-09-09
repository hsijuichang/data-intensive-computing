import os
import sys
import tempfile
import urllib.request
from pathlib import Path

_WINUTILS_URL = (
    "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe"
)
_HADOOP_DLL_URL = (
    "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll"
)


def _find_project_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate
        if (candidate / "data" / "raw").is_dir():
            return candidate
    raise FileNotFoundError("Could not find the project root")


def _ensure_winutils(hadoop_bin: Path) -> None:
    hadoop_bin.mkdir(parents=True, exist_ok=True)
    files = {
        "winutils.exe": _WINUTILS_URL,
        "hadoop.dll": _HADOOP_DLL_URL,
    }
    for name, url in files.items():
        target = hadoop_bin / name
        if target.exists() and target.stat().st_size > 0:
            continue
        urllib.request.urlretrieve(url, target)


def configure_windows_spark_env() -> None:
    """Set JAVA_HOME and HADOOP_HOME so Spark can start on Windows."""
    if sys.platform != "win32":
        return

    base_executable = Path(getattr(sys, "_base_executable", sys.executable))
    worker_python = base_executable if base_executable.is_file() else Path(sys.executable)
    os.environ["PYSPARK_PYTHON"] = str(worker_python)
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    # In notebooks the driver is the already-running kernel. Setting this
    # variable makes pyspark.cmd try to relaunch it and breaks paths with spaces.
    os.environ.pop("PYSPARK_DRIVER_PYTHON", None)

    java_home = Path(os.environ.get("JAVA_HOME", ""))
    if not (java_home / "bin" / "java.exe").exists():
        fallback = Path(r"D:\Eclipse Adoptium\jdk-17.0.20.101-hotspot")
        if (fallback / "bin" / "java.exe").exists():
            java_home = fallback
            os.environ["JAVA_HOME"] = str(java_home)
    if java_home.exists():
        os.environ["PATH"] = str(java_home / "bin") + os.pathsep + os.environ.get("PATH", "")

    project_root = _find_project_root(Path.cwd().resolve())
    hadoop_home = project_root / "tools" / "hadoop"
    _ensure_winutils(hadoop_home / "bin")
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["hadoop.home.dir"] = str(hadoop_home)
    os.environ["PATH"] = str(hadoop_home / "bin") + os.pathsep + os.environ.get("PATH", "")

    runtime_dir = Path(project_root.anchor) / "spark-tmp" / project_root.name
    runtime_dirs = {
        "SPARK_LOCAL_DIRS": runtime_dir / "local",
        "SPARK_JAVA_TMP_DIR": runtime_dir / "java-tmp",
        "SPARK_IVY_DIR": runtime_dir / "ivy",
        "SPARK_WAREHOUSE_DIR": runtime_dir / "warehouse",
        "HADOOP_TMP_DIR": runtime_dir / "hadoop-tmp",
        "JUPYTER_RUNTIME_DIR": runtime_dir / "jupyter-runtime",
        "IPYTHONDIR": runtime_dir / "ipython",
        "PIP_CACHE_DIR": runtime_dir / "pip-cache",
        "UV_CACHE_DIR": runtime_dir / "uv-cache",
    }
    for variable, directory in runtime_dirs.items():
        directory.mkdir(parents=True, exist_ok=True)
        os.environ[variable] = (
            directory.as_posix() if variable == "SPARK_JAVA_TMP_DIR" else str(directory)
        )

    os.environ["TEMP"] = str(runtime_dir / "local")
    os.environ["TMP"] = str(runtime_dir / "local")
    tempfile.tempdir = str(runtime_dir / "local")
