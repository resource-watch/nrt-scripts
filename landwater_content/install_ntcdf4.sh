#!/bin/bash

# Script to install hdf5 and netCDF4 libraries on a Linux Ubuntu system
# After: https://code.google.com/p/netcdf4-python/wiki/UbuntuInstall
# And http://unidata.github.io/netcdf4-python/ 

# You can check for newer version of the programs on 
# ftp://ftp.unidata.ucar.edu/pub/netcdf/netcdf-4/
# and other sources

BASHRC="~/.bashrc"

# Install zlib
v=1.2.8  
wget ftp://ftp.unidata.ucar.edu/pub/netcdf/netcdf-4/zlib-${v}.tar.gz
tar -xf zlib-${v}.tar.gz && cd zlib-${v}
./configure --prefix=/usr/local
#sudo make check install
make install
cd ..

# Install HDF5
v=1.8.13
wget ftp://ftp.unidata.ucar.edu/pub/netcdf/netcdf-4/hdf5-${v}.tar.gz
tar -xf hdf5-${v}.tar.gz && cd hdf5-${v}
prefix="/usr/local/hdf5-$v"
if [ $HDF5_DIR != $prefix ]; then
    echo "Add HDF5_DIR=$prefix to .bashrc"
    echo "" >> $BASHRC
    echo "# HDF5 libraries for python" >> $BASHRC
    echo export HDF5_DIR=$prefix  >> $BASHRC
fi
./configure --enable-shared --enable-hl --prefix=$HDF5_DIR
make -j 2 # 2 for number of procs to be used
make install
cd ..

# Install Netcdf
v=4.1.3
wget http://www.unidata.ucar.edu/downloads/netcdf/ftp/netcdf-${v}.tar.gz
tar -xf netcdf-${v}.tar.gz && cd netcdf-${v}
prefix="/usr/local/"
if [ $NETCDF4_DIR != $prefix ]; then
    echo "Add NETCDF4_DIR=$prefix to .bashrc"
    echo "" >> $BASHRC
    echo "# NETCDF4 libraries for python" >> $BASHRC
    echo export NETCDF4_DIR=$prefix  >> $BASHRC
fi
CPPFLAGS=-I$HDF5_DIR/include LDFLAGS=-L$HDF5_DIR/lib ./configure --enable-netcdf-4 --enable-shared --enable-dap --prefix=$NETCDF4_DIR
# make check
make 
make install
cd ..