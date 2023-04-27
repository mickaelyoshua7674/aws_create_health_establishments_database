install.packages("https://cran.r-project.org/src/contrib/Archive/read.dbc/read.dbc_1.0.5.tar.gz", repos = NULL)
library(read.dbc)

dbc_file_path <- Sys.getenv("DBC_FILE_PATH") # DBC_FILE_PATH=./PAPB2209.dbc CSV_FILE_PATH=./PAPB2209.csv Rscript dbc2csv.r
print(dbc_file_path)
csv_file_path <- Sys.getenv("CSV_FILE_PATH")
print(csv_file_path)

x <- read.dbc(dbc_file_path)
write.csv(x, csv_file_path, row.names = FALSE)
