import subprocess
from pathlib import Path

from .blast_command import Command

def _build_blast_format_command(out_format, archive, out=None):
    command = Command()
    command += ["blast_formatter"]
    command |= {"-archive": archive, "-outfmt": out_format}
    if out is not None:
        command.add_argument("-out", out)
    return command
        
def blast_format_file(out_format, archive: str | Path, out=None):
    command = _build_blast_format_command(out_format, archive, out)
    proc = subprocess.Popen(
        list(command.argument_iter()),
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL
    )
    res, _ = proc.communicate()
    if proc.returncode:        
        raise subprocess.CalledProcessError(proc.returncode, proc.args)
    return res

def blast_format_bytes(out_format, archive: bytes, out=None):
    command = _build_blast_format_command(out_format, "-", out)
    proc = subprocess.Popen(
        list(command.argument_iter()),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL
    )
    res, _ = proc.communicate(archive)
    if proc.returncode:        
        raise subprocess.CalledProcessError(proc.returncode, proc.args)
    return res        
