#ifndef SSB_SSB_H
#define SSB_SSB_H

#include <stdint.h>
#include <string>
#include <string.h>

namespace fdb {

class Lineorder {
public:
  int32_t orderkey;
  int8_t linenumber;
  int32_t custkey;
  int32_t partkey;
  int32_t suppkey;
  int32_t orderdate;
  char orderpriority[15];
  char shippriority[1];
  int8_t quantity;
  int32_t extendedprice;
  int32_t ordertotalprice;
  int8_t discount;
  int32_t revenue;
  int32_t supplycost;
  int8_t tax;
  int32_t commitdate; // uncertain
  char shipmode[10];

  Lineorder(){};

  void loadDataPiped(const std::string &line);
  void debugout() const;

  typedef int64_t PKType;
  PKType getPK() const { return (((int64_t) orderkey) << 8) + ((int64_t) linenumber);}
  static void extractPK(const void *tuple, void *key) {
    *(reinterpret_cast<PKType*> (key)) = reinterpret_cast<const Lineorder*>(tuple)->getPK();
  }
  static int compareTuplePK(const void *data1, const void *data2) {
    const Lineorder *l1 = reinterpret_cast<const Lineorder*> (data1);
    const Lineorder *l2 = reinterpret_cast<const Lineorder*> (data2);
    if (l1->orderkey != l2->orderkey) return l1->orderkey - l2->orderkey;
    return l1->linenumber - l2->linenumber;
  }
};

class Customer {
public:
  int32_t custkey;
  char name[25];
  char address[25];
  char city[10];
  char nation[15];
  char region[12];
  char phone[15];
  char mktsegment[10];

  Customer(){};

  void loadData(const std::string &line);
  void loadDataPiped(const std::string &line);
  void debugout() const;

  typedef int32_t PKType;
  PKType getPK() const { return custkey;}
  static void extractPK(const void *tuple, void *key) {
    *(reinterpret_cast<PKType*> (key)) = reinterpret_cast<const Customer*>(tuple)->custkey;
  }
};

class Supplier {
public:
  int32_t suppkey;
  char name[25];
  char address[25];
  char city[10];
  char nation[15];
  char region[12];
  char phone[15];

  Supplier(){};

  void loadDataPiped(const std::string &line);
  void debugout() const;

  typedef int32_t PKType;
  PKType getPK() const { return suppkey; }
  static void extractPK(const void *tuple, void *key) {
    *(reinterpret_cast<PKType*> (key)) = reinterpret_cast<const Supplier*>(tuple)->suppkey;
  }
};


class Part {
public:
  int32_t partkey;
  char name[22];
  char mfgr[6];
  char category[7];
  char brand[9];
  char color[11];
  char type[25];
  int8_t size;
  char container[10];

  Part(){};

  void loadDataPiped(const std::string &line);
  void debugout() const;

  typedef int32_t PKType;
  PKType getPK() const { return partkey; }
  static void extractPK(const void *tuple, void *key) {
    *(reinterpret_cast<PKType*> (key)) = reinterpret_cast<const Part*>(tuple)->partkey;
  }
};

class Date {
public:
  int32_t datekey;
  char date[18];
  char dayofweek[8];
  char month[9];
  int16_t year;
  int32_t yearmonthnum;
  char yearmonth[7];
  int8_t daynuminweek;
  int8_t daynuminmonth;
  int16_t daynuminyear;
  int8_t monthnuminyear;
  int8_t weeknuminyear;
  char sellingseason[12];
  int8_t lastdayinweekfl;
  int8_t lastdayinmonthfl;
  int8_t holidayfl;
  int8_t weekdayfl;

  Date(){};

  void loadDataPiped(const std::string &line);
  void debugout() const;

  typedef int32_t PKType;
  PKType getPK() const { return datekey; }
  static void extractPK(const void *tuple, void *key) {
    *(reinterpret_cast<PKType*> (key)) = reinterpret_cast<const Date*>(tuple)->datekey;
  }
};


class MVProjection {
public:
  //these columns are PK and also sort order of MVProjection.
  struct PKType {
    char s_region[12];
    int16_t d_year;
    char c_region[12];
    char s_nation[15];
    char c_nation[15];
    char s_city[10];
    char c_city[10];
    int32_t d_yearmonthnum;
    char d_yearmonth[7];
    int32_t l_orderkey; // appended for uniqueness
    int8_t l_linenumber; // appended for uniqueness

    inline bool operator==(const PKType &other) const {
      return ::memcmp(this, &other, sizeof(PKType)) == 0;
    }
    inline bool operator!=(const PKType &other) const {
      return ::memcmp(this, &other, sizeof(PKType)) != 0;
    }
    inline bool operator<(const PKType &other) const {
      return ::memcmp(this, &other, sizeof(PKType)) < 0;
    }
    inline int compare(const PKType &other) const {
      return ::memcmp(this, &other, sizeof(PKType));
    }
    static inline int keyCompareFunc (const void *key1, const void *key2) {
      const PKType *k1 = reinterpret_cast<const PKType*> (key1);
      const PKType *k2 = reinterpret_cast<const PKType*> (key2);
      return k1->compare(*k2);
    }
    static inline int keyDataCompareFunc (const void *key1, const void *data2) {
      const PKType *k1 = reinterpret_cast<const PKType*> (key1);
      const MVProjection *k2 = reinterpret_cast<const MVProjection*> (data2);
      return k1->compare(k2->key);
    }
  };

  PKType key;

  int8_t l_quantity;
  int32_t l_extendedprice;
  int8_t l_discount;
  int32_t l_revenue;
  int32_t l_supplycost;
  char p_mfgr[6];
  char p_category[7];
  char p_brand[9];
  int8_t d_weeknuminyear;

  MVProjection(){};
  MVProjection(const Lineorder &l, const Customer &c, const Date &d, const Part &p, const Supplier &s) {
    assign(l, c, d, p, s);
  }

  void assign(const Lineorder &l, const Customer &c, const Date &d, const Part &p, const Supplier &s) {
    ::memcpy (key.s_region, s.region, sizeof(key.s_region));
    key.d_year = d.year;
    ::memcpy (key.c_region, c.region, sizeof(key.c_region));
    ::memcpy (key.s_nation, s.nation, sizeof(key.s_nation));
    ::memcpy (key.c_nation, c.nation, sizeof(key.c_nation));
    ::memcpy (key.s_city, s.city, sizeof(key.s_city));
    ::memcpy (key.c_city, c.city, sizeof(key.c_city));
    key.d_yearmonthnum = d.yearmonthnum;
    ::memcpy (key.d_yearmonth, d.yearmonth, sizeof(key.d_yearmonth));
    key.l_orderkey = l.orderkey;
    key.l_linenumber = l.linenumber;

    l_quantity = l.quantity;
    l_extendedprice = l.extendedprice;
    l_discount = l.discount;
    l_revenue = l.revenue;
    l_supplycost = l.supplycost;
    ::memcpy (p_mfgr, p.mfgr, sizeof(p_mfgr));
    ::memcpy (p_category, p.category, sizeof(p_category));
    ::memcpy (p_brand, p.brand, sizeof(p_brand));
    d_weeknuminyear = d.weeknuminyear;
  }

  void debugout() const;

  PKType getPK() const { return key;}
  static void extractPK(const void *tuple, void *key) {
    *(reinterpret_cast<PKType*> (key)) = reinterpret_cast<const MVProjection*>(tuple)->key;
  }

  static int compareTuple(const void *tuple1, const void *tuple2) {
    return reinterpret_cast<const MVProjection*>(tuple1)->key.
        compare(reinterpret_cast<const MVProjection*>(tuple2)->key);
  }
};

} // fdb

#endif // SSB_SSB_H

