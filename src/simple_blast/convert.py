import os
import subprocess
from typing import Optional

from .blast_command import Command

def _build_blast_format_command(
        out_format: int | str,
        archive: str | os.PathLike,
        out: Optional[str | os.PathLike] = None,
) -> Command:
    command = Command()
    command += ["blast_formatter"]
    command |= {"-archive": archive, "-outfmt": out_format}
    if out is not None:
        command.add_argument("-out", out)
    return command
        
def blast_format_file(
        out_format: int | str,
        archive: str | os.PathLike,
        out: Optional[str | os.PathLike] = None
) -> bytes:
    """Format the BLAST archive at the given location with the given format.

    Optionally, an output file path may be provided. If none is provided, then
    the resulting formatted BLAST results will be returned as bytes.

    Parameters:
        out_format: The BLAST output format to use.
        archive:    A path to the Blast4 archive file (ASN.1) to format.
        out:        The output path.

    Returns:
        If no output path provided, the formatted BLAST results.
    """
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

def blast_format_bytes(
        out_format: int | str,
        archive: bytes,
        out: Optional[str | os.PathLike] = None
) -> bytes:
    """Format the BLAST archive, provided as bytes, with the given format.

    Optionally, an output file path may be provided. If none is provided, then
    the resulting formatted BLAST results will be returned as bytes.

    Parameters:
        out_format: The BLAST output format to use.
        archive:    The Blast4 archive file (ASN.1), as bytes, to format.
        out:        The output path.

    Returns:
        If no output path provided, the formatted BLAST results.
    """
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
