CREATE TABLE ${table_name} (
  bool boolean,

  tiny_int tinyint,
  small_int smallint,
  int_type int,
  big_int bigint,

  flt float,
  dbl double,
  dec decimal(3,2),

  str string,
  chr char(20),
  vchr varchar(20),

  ts timestamp,
  dat date,
  day_interval interval day,
  month_interval interval month,

  bin binary,

  struct1 struct<
    bool boolean,

    int_type int,
    big_int bigint,

    dbl double,
    dec decimal(6,4),

    str string,

    arr1: array<decimal(4,2)>,
    arr2: array<map<string, decimal(5,3)>>,
    arr3: array<struct<s: string, d: decimal(3,2)>>,

    map1: map<string, decimal(4,2)>,
    map2: map<string, array<decimal(5,3)>>,
    map3: map<string, struct<d: decimal(3,2), n: bigint>>,

    struct1: struct<s: string, d: decimal(4,2), n: bigint, a: array<decimal(5,3)>>
  >,

  arr1 array<decimal(4,2)>,
  arr2 array<map<string, decimal(5,4)>>,
  arr3 array<struct<s: string, d: decimal(3,2)>>,
  arr4 array<array<decimal(5,3)>>,

  map1 map<string, decimal(4,2)>,
  map2 map<string, array<decimal(5,3)>>,
  map3 map<string, struct<d: decimal(3,2), n: bigint>>,
  map4 map<string, array<bigint>>
)
