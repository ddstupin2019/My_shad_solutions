import typing as tp


def reformat_git_log(inp: tp.IO[str], out: tp.IO[str]) -> None:
    """Reads git log from `inp` stream, reformats it and prints to `out` stream

    Expected input format: `<sha-1>\t<date>\t<author>\t<email>\t<message>`
    Output format: `<first 7 symbols of sha-1>.....<message>`
    """
    inpStr = inp.read().split('\n')
    inpStr.pop()
    resultLog = ''
    for strLog in inpStr:
        x = 80 - 7 - len(strLog.split('\t')[-1])
        resultLog += strLog[:7] + ('.' * x) + strLog.split('\t')[-1] + '\n'
    out.write(resultLog)
