# ‘~/R/x86_64-amazon-linux-gnu-library/4.1’
# 'C:\Users\micka\AppData\Local/R/win-library/4.3'
# install.packages("languageserver")
# install.packages("devtools")
# devtools::install_url("https://cran.r-project.org/src/contrib/Archive/read.dbc/read.dbc_1.0.5.tar.gz")
# install.packages("https://cran.r-project.org/src/contrib/Archive/read.dbc/read.dbc_1.0.5.tar.gz", repos = NULL)


library(read.dbc)

dbc_file_path <- Sys.getenv("DBC_FILE_PATH")
print(dbc_file_path)
csv_file_path <- Sys.getenv("CSV_FILE_PATH")
print(csv_file_path)

x <- read.dbc(dbc_file_path)
print("Read.")
write.csv(x, csv_file_path, row.names = FALSE)
