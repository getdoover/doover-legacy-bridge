def get_connection_info(payload) -> dict | None:
    for k, v in payload["children"].items():
        if v["type"] == "uiConnectionInfo":
            return v

        try:
            children = v["children"]
        except KeyError:
            pass
        else:
            conn_info = get_connection_info(children)
            if conn_info is not None:
                return conn_info

    return None
