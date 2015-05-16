**This project is currently in the initial development phase mainly focused on research experiments. No working releases planned soon.**

BCB (Brown Column Base) is a [Column Store](http://db.csail.mit.edu/projects/cstore/) analogue of [BDB](http://www.oracle.com/technology/products/berkeley-db/index.html), an open-source embedded database library which is compact, flexible, super-fast and easy-to-use.

Like BDB, BCB is a C/C++ library statically or dynamically linked to user programs, not an independent system itself (at least for main use). This allows the maximum amount of simplicity, flexibility, ease of use and performance at the cost of declarative language (SQL), query optimizer, GUI, etc (although those could be built on top of BCB).

The source code of BCB is a branch of a  column store implementation at Brown University for research experiments. We will keep the two code bases synced but separated so that (sometimes messy) experimental trials do not mix into BCB while BCB employs the best-performing techniques out of the research work.