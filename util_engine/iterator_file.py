import itertools
import math
import types


def prime_check(n):
    if abs(n) % 1 > 1e-15:
        return False
    elif n < 2:
        return False
    elif n == 2 or n == 3:
        return True
    elif n % 2 == 0 or n % 3 == 0:
        return False
    else:
        i = 5
        while i * i <= n:
            if n % i == 0 or n % (i + 2) == 0:
                return False
            i += 6
        return True

def composite_check(n):
    if abs(n) % 1 > 1e-15:
        return False
    elif prime_check(n):
        return False
    elif n < 2:
        return False
    else:
        return True

def is_divisible(a, b):
    if b == 0:
        return False  # error free way
    q = a / b
    return abs(q % 1) < 1e-15

def power_of(n,k):
    if not float(n).is_integer():
        return False
    if not isinstance(k, int):
        raise TypeError("k must be an integer")
    n = int(n)
    k = int(k)
    if k == 0:
        return True if n == 0 else False
    elif k == 1:
        return True if n == 1 else False
    elif k == -1:
        return True if abs(n) == 1 else False
    elif k > 0 and n < 0:
        return False
    elif n == 1:
        return True
    else:
        sign_n = 1 if n > 0 else -1
        sign_k = 1 if k > 0 else -1
        n = abs(int(n))
        k = abs(int(k))
        count = 0
        while n != 1:
            if n % k == 0:
                n //= k
                count += 1
            else:
                return False

        if sign_k < 0:
            if count % 2 == 0 and sign_n < 0:
                return False
            elif count % 2 == 1 and sign_n > 0:
                return False

        return True

def square_check(n):
    if not float(n).is_integer():
        return False
    elif n < 0:
        return False
    n = round(n)
    return round(n ** (1 / 2)) ** 2 == n

def cube_check(n):
    if not float(n).is_integer():
        return False
    n = round(n)
    sign_n = 1 if n >= 0 else -1
    return (sign_n * round(abs(n) ** (1/3))) ** 3 == n

def nth_power_check(n, k):
    if not float(n).is_integer():
        return False
    elif not isinstance(k, int):
        raise TypeError("k must be an integer")
    elif k == 0:
        return False
    elif n < 0 and k % 2 == 0:
        return False
    n = round(n)
    sign_n = 1 if n >= 0 else -1
    return (sign_n * round(abs(n) ** (1/k))) ** k == n

def coprime_check(a,b):
    if not (float(a).is_integer() or float(b).is_integer()):
        return False
    else:
        return math.gcd(a,b) == 1

def find_factors(n: int, pure=False) -> list[int]:
    """
    Returns all positive factors of an Integer.To get both negative and positive factors set pure=True
    :param n: The Integer whose factors are to be found.
    :param pure: Functions returns all factors, both -ve and +ve, if pure=True.
    :return: A sorted list of factors
    """

    if not float(n).is_integer():
        return []
    n = round(n)

    if n == 0:
        raise ValueError("0 has infinitely many factors.")

    n = abs(n)
    result = []
    for i in range(1, int(math.sqrt(n)) + 1):
        if n % i == 0:
            result.append(i)
            if n // i != i:
                result.append(n // i)

    if pure:
        result.extend([-i for i in result])

    return sorted(result)

def perfect_check(n):
    return float(n).is_integer() and sum(find_factors(n)) == n

def abundancy_check(n,neg=False):
    if not float(n).is_integer():
        return False
    n = round(n)
    if neg:
        return sum(find_factors(n)) > abs(n)
    elif n <= 0:
        return False
    else:
        return sum(find_factors(n)) > n

def deficiency_check(n, neg=False):
    if not float(n).is_integer():
        return False
    n = round(n)
    if neg:
        return sum(find_factors(n)) < abs(n)
    elif n <= 0:
        return False
    else:
        return sum(find_factors(n)) < n

def int_or_zero(n):
    try:
        return int(n)
    except ValueError:
        return 0

def palindrome_check(n):
    if n < 0:
        return False
    return str(n) == str(n)[::-1]

class CustomIterable:
    def __init__(self, stop_start,stop=None,step=1):
        if isinstance(stop_start,(tuple,list, types.GeneratorType)):
            values = stop_start
        elif stop is None:
            values = range(0,stop_start,step)
        else:
            values = range(stop_start,stop,step)

        self.values = values

    def __iter__(self):
        self.values, copy_gen = itertools.tee(self.values)
        return copy_gen

    def is_even(self):
        return CustomIterable((i for i in self.values if i % 2 == 0 and abs(i) % 1 < 1e-15))

    def skip_even(self):
        return CustomIterable((i for i in self.values if i % 2 != 0 and abs(i) % 1 < 1e-15))

    def is_odd(self):
        return CustomIterable((i for i in self.values if i % 2 == 1 and abs(i) % 1 < 1e-15))

    def skip_odd(self):
        return CustomIterable((i for i in self.values if i % 2 != 1 and abs(i) % 1 < 1e-15))

    def is_prime(self):
        return CustomIterable((i for i in self.values if prime_check(i)))

    def skip_prime(self):
        return CustomIterable((i for i in self.values if not prime_check(i)))

    def is_composite(self):
        return CustomIterable((i for i in self.values if composite_check(i)))

    def skip_composite(self):
        return CustomIterable((i for i in self.values if not composite_check(i)))

    def is_divisible_by(self,divisor):
        return CustomIterable((i for i in self.values if is_divisible(i,divisor)))

    def skip_divisible_by(self,divisor):
        return CustomIterable((i for i in self.values if not is_divisible(i, divisor)))

    def is_greater_than(self,n):
        return CustomIterable((i for i in self.values if i > n))

    def skip_greater_than(self,n):
        return CustomIterable((i for i in self.values if i <= n))

    def is_less_than(self,n):
        return CustomIterable((i for i in self.values if i < n))

    def skip_less_than(self,n):
        return CustomIterable((i for i in self.values if i >= n))

    def is_between(self,a,b):
        return CustomIterable((i for i in self.values if a < i < b))

    def skip_between(self,a,b):
        return CustomIterable((i for i in self.values if not (a < i < b)))

    def is_inclusive_between(self,a,b):
        return CustomIterable((i for i in self.values if a <= i <= b))

    def skip_inclusive_between(self,a,b):
        return CustomIterable((i for i in self.values if not (a <= i <= b)))

    def is_equal_to(self,k):
        return CustomIterable((i for i in self.values if i == k))

    def skip_equal_to(self,k):
        return CustomIterable((i for i in self.values if i != k))

    def is_positive(self):
        return CustomIterable((i for i in self.values if i > 0))

    def skip_positive(self):
        return CustomIterable((i for i in self.values if i <= 0))

    def is_negative(self):
        return CustomIterable((i for i in self.values if i < 0))

    def skip_negative(self):
        return CustomIterable((i for i in self.values if i >= 0))

    def is_zero(self):
        return CustomIterable((i for i in self.values if i == 0))

    def skip_zero(self):
        return CustomIterable((i for i in self.values if i != 0))

    def is_power_of(self, power):
        return CustomIterable((i for i in self.values if power_of(i, power)))

    def skip_power_of(self, power):
        return CustomIterable((i for i in self.values if not power_of(i, power)))

    def is_square(self):
        return CustomIterable((i for i in self.values if square_check(i)))

    def skip_square(self):
        return CustomIterable((i for i in self.values if not square_check(i)))

    def is_cube(self):
        return CustomIterable((i for i in self.values if cube_check(i)))

    def skip_cube(self):
        return CustomIterable((i for i in self.values if not cube_check(i)))

    def is_nth_power(self, n):
        return CustomIterable((i for i in self.values if nth_power_check(i, n)))

    def skip_nth_power(self, n):
        return CustomIterable((i for i in self.values if not nth_power_check(i, n)))

    def is_coprime_to(self, n):
        return CustomIterable((i for i in self.values if coprime_check(i, n)))

    def skip_coprime_to(self, n):
        return CustomIterable((i for i in self.values if not coprime_check(i, n)))

    def map(self, func, *params):
        return CustomIterable((func(i, *params) for i in self.values))

    def has_factor_count(self, minimum=0, maximum=2):
        return CustomIterable((i for i in self.values if float(i).is_integer() and minimum <= len(find_factors(i)) <= maximum))

    def is_perfect_number(self):
        return CustomIterable((i for i in self.values if perfect_check(i)))

    def skip_perfect_number(self):
        return CustomIterable((i for i in self.values if not perfect_check(i)))

    def is_abundant(self, allow_negatives=False):
        return CustomIterable((i for i in self.values if abundancy_check(i,neg=allow_negatives)))

    def skip_abundant(self, allow_negatives=False):
        return CustomIterable((i for i in self.values if not abundancy_check(i,neg=allow_negatives)))

    def is_deficient(self, allow_negatives=False):
        return CustomIterable((i for i in self.values if deficiency_check(i,neg=allow_negatives)))

    def skip_deficient(self, allow_negatives=False):
        return CustomIterable((i for i in self.values if not deficiency_check(i,neg=allow_negatives)))

    def contains(self, n):
        return CustomIterable((i for i in self.values if str(n) in str(i)))

    def does_not_contain(self, n):
        return CustomIterable((i for i in self.values if str(n) not in str(i)))

    def has_digit_sum(self, n):
        return CustomIterable((i for i in self.values if sum(map(int_or_zero, str(i))) == n))

    def skip_digit_sum(self, n):
        return CustomIterable((i for i in self.values if sum(map(int_or_zero, str(i))) != n))

    def skip_first(self, n):
        value_list = list(self.values)
        return CustomIterable(value_list[n:] if len(value_list) > n+1 else [])

    def keep_first(self, n):
        value_list = list(self.values)
        return CustomIterable(value_list[:n] if len(value_list) > n else value_list)

    def skip_last(self, n):
        value_list = list(self.values)
        return CustomIterable(value_list[:-n] if len(value_list) > n else [])

    def keep_last(self, n):
        value_list = list(self.values)
        return CustomIterable(value_list[-n:] if len(value_list) > n else value_list)

    def skip_first_and_last(self, n):
        value_list = list(self.values)
        return CustomIterable(value_list[n:-n] if len(value_list) > 2*n+1 else [])

    def keep_every(self, n):
        value_list = list(self.values)
        return CustomIterable(value_list[::n] if len(value_list) > n else [])

    def skip_every(self, n):
        value_list = list(self.values)
        return CustomIterable([v for i,v in enumerate(value_list) if (i+1) % n != 0])

    def skip_subset(self,a,b):
        if not isinstance(a,int) or not isinstance(b,int):
            raise ValueError('a and b must be int')

        if a > b:
            raise ValueError('a must be less than b')

        elif a == b:
            return CustomIterable(self.values)

        value_list = list(self.values)
        b = min(b, len(value_list))
        a = max(a, 0)

        return CustomIterable(value_list[0:a] + (value_list[b:]))

    def is_palindromic(self):
        return CustomIterable((i for i in self.values if palindrome_check(i)))

    def filter(self,func):
        return CustomIterable((i for i in self.values if func(i)))




print(sum([0]))


