class LifeGame(object):
    """
    Class for Game life
    """

    def __init__(self, board: list[list[int]]):
        self.board = board
        self.n = len(board)
        self.m = 0 if self.n == 0 else len(board[0])

    def _col_neighbors(self, x: int, y: int, type_n: int) -> int:
        sd = [(i, j) for i in range(-1, 2) for j in range(-1, 2)]
        sd.remove((0,0))
        r = 0
        for si, sj in sd:
            i = x + si
            j = y + sj
            if 0 <= i < self.n and 0 <= j < self.m:
                if self.board[i][j] == type_n:
                    r += 1
        return r

    def _update_cl(self, x: int, y: int) -> int:
        if self.board[x][y] == 0:
            if self._col_neighbors(x, y, 2) == 3:
                return 2
            elif self._col_neighbors(x, y, 3) == 3:
                return 3
        elif (self.board[x][y] == 2 and
              (self._col_neighbors(x, y, 2) <= 1 or self._col_neighbors(x, y, 2) >= 4)):
            return 0
        elif (self.board[x][y] == 3 and
              (self._col_neighbors(x, y, 3) <= 1 or self._col_neighbors(x, y, 3) >= 4)):
            return 0
        return self.board[x][y]

    def get_next_generation(self):
        new_board = [[0] * self.m for i in range(self.n)]
        for i in range(self.n):
            for j in range(self.m):
                new_board[i][j] = self._update_cl(i,j)
        self.board = new_board
        return self.board

