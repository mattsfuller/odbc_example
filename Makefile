CC=gcc

all: odbc.c clean
	$(CC) -I/usr/include odbc.c -O0 -g -o odbc -lodbc -lodbcinst -lodbccr

clean: 
	rm -rf odbc
