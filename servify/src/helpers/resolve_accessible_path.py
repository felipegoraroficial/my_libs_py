import glob


def resolve_accessible_path(path: str, dbutils) -> str:
    """
    Valida/resolve um path para leitura DBFS ou 'file:'.
    - Com wildcard: garante que existe ao menos um arquivo, mantém wildcard para leitura.
    - Sem wildcard: tenta DBFS, se não, tenta 'file:' se nada der certo, lança FileNotFoundError
    """

    if "*" in path:
        arquivos = glob.glob(path.replace("file:", ""))
        if not arquivos:
            raise FileNotFoundError(f"No file founded in: {path}")

        primeiro = arquivos[0]
        try:
            dbutils.fs.ls(primeiro)
        except Exception:
            arquivo_file = f"file:{primeiro}"
            try:
                dbutils.fs.ls(arquivo_file)
            except Exception as exc_file:
                raise FileNotFoundError(
                    f"File '{arquivo_file}' is not accessible by DBFS netheir 'file:'."
                ) from exc_file

        return path

    try:
        dbutils.fs.ls(path)
        return path
    except Exception:
        path_file = f"file:{path}"
        try:
            dbutils.fs.ls(path_file)
            return path_file
        except Exception as exc_file:
            raise FileNotFoundError(
                f"File '{path_file}' is not accessible by DBFS netheir 'file:'."
            ) from exc_file
