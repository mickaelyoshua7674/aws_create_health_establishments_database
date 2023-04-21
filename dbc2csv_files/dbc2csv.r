install.packages("https://cran.r-project.org/src/contrib/Archive/read.dbc/read.dbc_1.0.5.tar.gz", repos = NULL)

library(read.dbc)
dbc_file_path <- Sys.getenv("DBC_FILE_PATH") # "./dbc_files/PAPB2201.dbc"
csv_file_path <- Sys.getenv("CSV_FILE_PATH") # "./csv_files/PAPB2201.csv"
x <- read.dbc(dbc_file_path)
write.csv(x, csv_file_path, row.names = FALSE)