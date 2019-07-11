def handle(req):
    """handle a request to the function
    Args:
        req (str): request body
    """
    str1 = req.decode('utf-8')
    str1 = "nxp test:" + str1

    return bytes(str1, encoding='utf-8')
