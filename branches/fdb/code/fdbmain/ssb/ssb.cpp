#include "ssb.h"

#include <string.h>
#include <iostream>
#include <sstream>
#include <boost/tokenizer.hpp>

namespace fdb {

inline void copyStr(char *dest, const boost::tokenizer<boost::char_separator<char> >::const_iterator &it) {
  size_t s = it->size();
  ::memcpy (dest, it->data(), s);
}

void Lineorder::loadDataPiped(const std::string &line) {
  ::memset (this, 0, sizeof(Lineorder));
  boost::char_separator<char> sep("|", "", boost::keep_empty_tokens);
  boost::tokenizer<boost::char_separator<char> > tokenizer (line, sep);
  boost::tokenizer<boost::char_separator<char> >::const_iterator it = tokenizer.begin();
  orderkey = ::atol((it++)->c_str());
  linenumber = ::atol((it++)->c_str());
  custkey = ::atol((it++)->c_str());
  partkey = ::atol((it++)->c_str());
  suppkey = ::atol((it++)->c_str());
  orderdate = ::atol((it++)->c_str());
  copyStr (orderpriority, (it++));
  copyStr (shippriority, (it++));
  quantity = ::atol((it++)->c_str());
  extendedprice = ::atol((it++)->c_str());
  ordertotalprice = ::atol((it++)->c_str());
  discount = ::atol((it++)->c_str());
  revenue = ::atol((it++)->c_str());
  supplycost = ::atol((it++)->c_str());
  tax = ::atol((it++)->c_str());
  commitdate = ::atol((it++)->c_str());
  copyStr (shipmode, (it++));
}

void Lineorder::debugout() const {
  std::cout << "Lineorder: "
    << "orderkey=" << orderkey << ","
    << "linenumber=" << linenumber << ","
    << "custkey=" << custkey << ","
    << "partkey=" << partkey << ","
    << "suppkey=" << suppkey << ","
    << "orderdate=" << orderdate << ","
    << "orderpriority=" << orderpriority << ","
    << "shippriority=" << shippriority << ","
    << "quantity=" << quantity << ","
    << "extendedprice=" << extendedprice << ","
    << "ordertotalprice=" << ordertotalprice << ","
    << "discount=" << discount << ","
    << "revenue=" << revenue << ","
    << "supplycost=" << supplycost << ","
    << "tax=" << tax << ","
    << "commitdate=" << commitdate << ","
    << "shipmode=" << shipmode << std::endl;
}


void Customer::loadDataPiped(const std::string &line) {
  ::memset (this, 0, sizeof(Customer));
  boost::char_separator<char> sep("|", "", boost::keep_empty_tokens);
  boost::tokenizer<boost::char_separator<char> > tokenizer (line, sep);
  boost::tokenizer<boost::char_separator<char> >::const_iterator it = tokenizer.begin();
  custkey = ::atol((it++)->c_str());
  copyStr (name, (it++));
  copyStr (address, (it++));
  copyStr (city, (it++));
  copyStr (nation, (it++));
  copyStr (region, (it++));
  copyStr (phone, (it++));
  copyStr (mktsegment, (it++));
}

void Customer::debugout() const {
  std::cout << "Customer: "
    << "custkey=" << custkey << ","
    << "name=" << name << ","
    << "address=" << address << ","
    << "city=" << city << ","
    << "nation=" << nation << ","
    << "region=" << region << ","
    << "phone=" << phone << ","
    << "mktsegment=" << mktsegment << std::endl;
}

void Supplier::loadDataPiped(const std::string &line) {
  ::memset (this, 0, sizeof(Supplier));
  boost::char_separator<char> sep("|", "", boost::keep_empty_tokens);
  boost::tokenizer<boost::char_separator<char> > tokenizer (line, sep);
  boost::tokenizer<boost::char_separator<char> >::const_iterator it = tokenizer.begin();
  suppkey = ::atol((it++)->c_str());
  copyStr (name, (it++));
  copyStr (address, (it++));
  copyStr (city, (it++));
  copyStr (nation, (it++));
  copyStr (region, (it++));
  copyStr (phone, (it++));
}

void Supplier::debugout() const {
  std::cout << "Supplier: "
    << "suppkey=" << suppkey << ","
    << "name=" << name << ","
    << "address=" << address << ","
    << "city=" << city << ","
    << "nation=" << nation << ","
    << "region=" << region << ","
    << "phone=" << phone << std::endl;
}

void Part::loadDataPiped(const std::string &line) {
  ::memset (this, 0, sizeof(Part));
  boost::char_separator<char> sep("|", "", boost::keep_empty_tokens);
  boost::tokenizer<boost::char_separator<char> > tokenizer (line, sep);
  boost::tokenizer<boost::char_separator<char> >::const_iterator it = tokenizer.begin();
  partkey = ::atol((it++)->c_str());
  copyStr (name, (it++));
  copyStr (mfgr, (it++));
  copyStr (category, (it++));
  copyStr (brand, (it++));
  copyStr (color, (it++));
  copyStr (type, (it++));
  size = ::atol((it++)->c_str());
  copyStr (container, (it++));
}

void Part::debugout() const {
  std::cout << "Part: "
    << "partkey=" << partkey << ","
    << "name=" << name << ","
    << "mfgr=" << mfgr << ","
    << "category=" << category << ","
    << "brand=" << brand << ","
    << "color=" << color << ","
    << "type=" << type << ","
    << "size=" << size << ","
    << "container=" << container << std::endl;
}

void Date::loadDataPiped(const std::string &line) {
  ::memset (this, 0, sizeof(Date));
  boost::char_separator<char> sep("|", "", boost::keep_empty_tokens);
  boost::tokenizer<boost::char_separator<char> > tokenizer (line, sep);
  boost::tokenizer<boost::char_separator<char> >::const_iterator it = tokenizer.begin();
  datekey = ::atol((it++)->c_str());
  copyStr (date, (it++));
  copyStr (dayofweek, (it++));
  copyStr (month, (it++));
  year = ::atol((it++)->c_str());
  yearmonthnum = ::atol((it++)->c_str());
  copyStr (yearmonth, (it++));
  daynuminweek = ::atol((it++)->c_str());
  daynuminmonth = ::atol((it++)->c_str());
  daynuminyear = ::atol((it++)->c_str());
  monthnuminyear = ::atol((it++)->c_str());
  weeknuminyear = ::atol((it++)->c_str());
  copyStr (sellingseason, (it++));
  lastdayinweekfl = ::atol((it++)->c_str());
  lastdayinmonthfl = ::atol((it++)->c_str());
  holidayfl = ::atol((it++)->c_str());
  weekdayfl = ::atol((it++)->c_str());
}

void Date::debugout() const {
  std::cout << "Date: "
    << "datekey=" << datekey << ","
    << "date=" << date << ","
    << "dayofweek=" << dayofweek << ","
    << "month=" << month << ","
    << "year=" << year << ","
    << "yearmonthnum=" << yearmonthnum << ","
    << "yearmonth=" << yearmonth << ","
    << "daynuminweek=" << daynuminweek << ","
    << "daynuminmonth=" << daynuminmonth << ","
    << "daynuminyear=" << daynuminyear << ","
    << "monthnuminyear=" << monthnuminyear << ","
    << "weeknuminyear=" << weeknuminyear << ","
    << "sellingseason=" << sellingseason << ","
    << "lastdayinweekfl=" << lastdayinweekfl << ","
    << "lastdayinmonthfl=" << lastdayinmonthfl << ","
    << "holidayfl=" << holidayfl << ","
    << "weekdayfl=" << weekdayfl << std::endl;
}

} //fdb
