#!/bin/bash

# ===============================================================================
# Copyright (c)  2014 Geoscience Australia
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither Geoscience Australia nor the names of its contributors may be
#       used to endorse or promote products derived from this software
#       without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#===============================================================================

PBS_SCRIPT="$HOME/git_checkouts/agdc/api-examples/source/main/python/workflow/arg25_stats.pbs.sh"

OUTPUT_DIR="/g/data/u46/vmn547/tmp/example/arg25_big_memory"

qsub -v outputdir="${OUTPUT_DIR}",xmin=146,xmax=146,ymin=-34,ymax=-34,acqmin=1987-01,acqmax=2014-12,chunkx=200,chunky=200,add_on_name=_big_memory "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=121,xmax=121,ymin=-29,ymax=-29,acqmin=1987,acqmax=2014,chunkx=200,chunky=200,add_on_name=_new "${PBS_SCRIPT}"

#qsub -v outputdir="${OUTPUT_DIR}",xmin=121,xmax=121,ymin=-29,ymax=-29,acqmin=1987-01,acqmax=2014-12,chunkx=200,chunky=200,percentile=90,add_on_name=_new_api "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=121,xmax=121,ymin=-29,ymax=-29,acqmin=1987-01,acqmax=2014-12,chunkx=200,chunky=200,percentile=95,add_on_name=_new_api "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=118,xmax=118,ymin=-23,ymax=-23,acqmin=1987-01,acqmax=2014-12,chunkx=1000,chunky=1000,percentile=95,add_on_name=_full "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=135,xmax=135,ymin=-18,ymax=-18,acqmin=1987-01,acqmax=2014-12,chunkx=1000,chunky=1000,percentile=95,add_on_name=_full "${PBS_SCRIPT}"
#qsub -v outputdir="${OUTPUT_DIR}",xmin=142,xmax=142,ymin=-22,ymax=-22,acqmin=1987-01,acqmax=2014-12,chunkx=1000,chunky=1000,percentile=95,add_on_name=_full  "${PBS_SCRIPT}"
