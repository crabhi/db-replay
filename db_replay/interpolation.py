import bisect
import warnings
from typing import Iterable, Tuple


class PolyLine:
    """
    Similar to numpy.interp, defines a function by points, with linear interpolation
    in between them and constant value outside the domain.

    This class existis to avoid depending on Numpy.
    """
    def __init__(self, points: Iterable[Tuple[float, float]]):
        self.xs, self.ys = list(zip(*sorted(points, key=lambda p: p[0])))

        if len(self.xs) < 1:
            raise ValueError('No points supplied')

        if len(self.xs) < 2:
            warnings.warn('Only single point supplied. The function will be constant.')

        if len(set(self.xs)) != len(self.xs):
            raise ValueError('Duplicate X values not allowed')

    def evaluate(self, x):
        """
        Evaluate the function at point x.

        Example:

        >>> f = PolyLine([(1., 5.), (2., 7.), (3., 1.)])
        >>> f.evaluate(-700)
        5.0
        >>> f.evaluate(1)
        5.0
        >>> f.evaluate(2)
        7.0
        >>> f.evaluate(3)
        1.0
        >>> f.evaluate(40)
        1.0
        >>> f.evaluate(1.5)
        6.0
        >>> f.evaluate(2.5)
        4.0
        """
        if x <= self.xs[0]:
            return self.ys[0]
        elif x >= self.xs[-1]:
            return self.ys[-1]

        i = bisect.bisect_right(self.xs, x)
        slope = self.ys[i] - self.ys[i - 1] / (self.xs[i] - self.xs[i - 1])

        return self.ys[i - 1] + (x - self.xs[i - 1]) * slope
