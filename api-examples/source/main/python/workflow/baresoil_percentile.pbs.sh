#!/bin/bash
#PBS -N soil
#PBS -P u46
#PBS -q express
#PBS -l ncpus=16,mem=32GB
#PBS -l walltime=04:00:00
#PBS -l wd
#PBS -joe -o $outputdir
##PBS -l other=gdata1

export MODULEPATH=/projects/u46/opt/modules/modulefiles:$MODULEPATH
export -p PYTHONPATH=$HOME/git_checkouts/agdc/api-examples/source/main/python:$HOME/git_checkouts/agdc/api/source/main/python:$HOME/tmp/enum34-1.0-py2.7.egg:$PYTHONPATH

module unload python
module load python/2.7.6
module load enum34
module load psutil
module load psycopg2
module load gdal
module load luigi-mpi
module load numpy/1.9.1

module list

echo $PYTHONPATH

COMMAND="python $HOME/git_checkouts/agdc/api-examples/source/main/python/workflow/baresoil_percentile.py --output-dir $outputdir --x-min $xmin --x-max $xmax --y-min $ymin --y-max $ymax --acq-min $acqmin --acq-max $acqmax --mask-pqa-apply --mask-wofs-apply --chunk-size-x $chunkx --chunk-size-y $chunky --percentile $percentile --add_on_name $add_on_name"

# MPI
#mpirun -np 16 $COMMAND

# NO MPI
$COMMAND --local-scheduler --workers 16