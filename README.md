# cs342spring2020-p2
## Description

This files includes my (ID: 21701546) submission for cs342 (Operating Systems course) project 2 in Spring 2020. The process multiplies a matrix by a vector and saves the result into a file. The process using multithreading to multiply the matrices quickly.

## Usage
To compile, do:

```
make
```

To run,  

```
mvt_s <matrix_file> <vector_file> <result_fule>
```

and <matrix_file> is in sparse matrix format (lines: i j v, i is row, j is column, v is value), <vector_file> is a vector. The result is written to <result_file> in the same format.
