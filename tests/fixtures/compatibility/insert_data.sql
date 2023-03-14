INSERT INTO ${table_name} (
  bool,

  tiny_int,
  small_int,
  int_type,
  big_int,

  flt,
  dbl,
  dec,

  str,
  chr,
  vchr,

  ts,
  dat,
  day_interval,
  month_interval,

  bin,

  struct1,

  arr1,
  arr2,
  arr3,
  arr4,

  map1,
  map2,
  map3,
  map4
) VALUES (
  true,

  127,
  32000,
  4000000,
  372036854775807,

  1.4142,
  2.71828182,
  3.14,

  'string value',
  'char value',
  'varchar value',

  '2014-01-17 00:17:13',
  '2014-01-17',
  INTERVAL '1' day,
  INTERVAL '1' month,

  'binary value',

  named_struct(
    'bool', false,

    'int_type', 4000000,
    'big_int', 372036854775807,

    'dbl', 2.71828182,
    'dec', 1.4142,

    'str', 'string value',

    'arr1', array(1.41, 2.71, 3.14),
    'arr2', array(map('sqrt2', 1.4142), map('e', 2.7182), map('pi', 3.1415)),
    'arr3', array(
      named_struct('s', 'e', 'd', 2.71),
      named_struct('s', 'pi', 'd', 3.14)
    ),

    'map1', map('sqrt2', 1.41, 'e', 2.71, 'pi', 3.14),
    'map2', map('arr1', array(1.414), 'arr2', array(2.718, 3.141)),
    'map3', map(
      'struct1',
      named_struct('d', 3.14, 'n', 314159265359),
      'struct2',
      named_struct('d', 2.71, 'n', 271828182846)
    ),

    'struct1', named_struct(
      's', 'string value',
      'd', 3.14,
      'n', 314159265359,
      'a', array(2.718, 3.141)
    )
  ),

  array(1.41, 2.71, 3.14),
  array(map('sqrt2', 1.4142), map('e', 2.7182), map('pi', 3.1415)),
  array(
    named_struct('s', 'sqrt2', 'd', 1.41),
    named_struct('s', 'e', 'd', 2.71),
    named_struct('s', 'pi', 'd', 3.14)
  ),
  array(array(1.414), array(2.718, 3.141)),

  map('sqrt2', 1.41, 'e', 2.71, 'pi', 3.14),
  map('arr1', array(1.414), 'arr2', array(2.718, 3.141)),
  map(
    'struct1',
    named_struct('d', 3.14, 'n', 314159265359),
    'struct2',
    named_struct('d', 2.71, 'n', 271828182846)
  ),
  map('e', array(271828182846), 'pi', array(314159265359))
)
