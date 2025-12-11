def caesar_encrypt(message: str, n: int) -> str:
    """Encrypt message using caesar cipher

    :param message: message to encrypt
    :param n: shift
    :return: encrypted message
    """
    result: list[str] = []

    for elem in message:
        numElem = ord(elem)
        if elem.islower():
            numElem = (ord(elem) + n - ord('a')) % 26 + ord('a')
        elif elem.isupper():
            numElem = (ord(elem) + n - ord('A')) % 26 + ord('A')
        result.append(chr(numElem))
    return ''.join(result)
