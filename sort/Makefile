.PHONY: all clean

MPICC = mpic++
CFLAGS = -O3 -std=c++11 -mavx -Weffc++ -pedantic -Wall -Wextra -fopenmp #-DDEBUG_ENABLE -g
CCFLAGS = -O3 -std=c99 -mavx -pedantic -Wall -Wextra
SRCFILES = sort.cc fileserver.cc io_forwarder.cc compute_node.cc utils.cc \
           compression/src/bitpacking.cc compression/src/integratedbitpacking.cc \
           compression/src/intersection.cc compression/src/simdbitpacking.cc \
           compression/src/simdintegratedbitpacking.cc compression/src/usimdbitpacking.cc \
           
HFILES = dep_sort_config.h utils.h
INCLUDES = -Icompression/include
OBJFILES = $(patsubst %.cc,%.o,$(SRCFILES)) compression/src/varintdecode.o
EXEFILES = sort

all: $(EXEFILES)

%.o: %.cc $(HFILES)
	$(MPICC) $(INCLUDES) $(CFLAGS) -o $@ -c $<

compression/src/varintdecode.o:
	gcc $(CCFLAGS) compression/src/varintdecode.c -o compression/src/varintdecode.o -c

sort: $(OBJFILES)
	$(MPICC) $(CFLAGS) -o $@ $^

clean:
	rm -f $(EXEFILES) $(OBJFILES) sorted.dat data.bin*
