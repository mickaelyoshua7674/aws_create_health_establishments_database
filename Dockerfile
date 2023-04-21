FROM r-base
COPY ./install_read_dbc.r ./usr/local/src/myscripts/
WORKDIR /usr/local/src/myscripts/
RUN ["Rscript", "install_read_dbc.r"]