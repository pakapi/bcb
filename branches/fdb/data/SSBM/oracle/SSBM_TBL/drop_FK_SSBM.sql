
alter table lineorder
  drop constraint custconstr;
alter table lineorder
  add constraint partconstr;
alter table lineorder
  add constraint suppconstr;
alter table lineorder
  add constraint dateconstr;
