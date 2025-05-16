#!/bin/sh
# This wrapper script is intended to be submitted to HTCondor to support
# communicating jobs.
#
# This script uses the following environment variables set by the submit MATLAB code:
# PARALLEL_SERVER_CMR         - the value of ClusterMatlabRoot (may be empty)
# PARALLEL_SERVER_MATLAB_EXE  - the MATLAB executable to use
# PARALLEL_SERVER_MATLAB_ARGS - the MATLAB args to use
# PARALLEL_SERVER_TOTAL_TASKS - total number of workers to start
# PARALLEL_SERVER_NUM_THREADS - number of cores needed per worker
# PARALLEL_SERVER_DEBUG       - used to debug problems on the cluster
#
# The following environment variables are forwarded through mpiexec:
# PARALLEL_SERVER_DECODE_FUNCTION     - the decode function to use
# PARALLEL_SERVER_STORAGE_LOCATION    - used by decode function
# PARALLEL_SERVER_STORAGE_CONSTRUCTOR - used by decode function
# PARALLEL_SERVER_JOB_LOCATION        - used by decode function

# Copyright 2020-2025 The MathWorks, Inc.

# Find out what condor proc number we are and how many condor procs there are.
_CONDOR_PROCNO=$_CONDOR_PROCNO
_CONDOR_NPROCS=$_CONDOR_NPROCS

if [ $_CONDOR_PROCNO -ne 0 ] ; then
    # Reserve Slot for MPIEXEC processing
    exit 0
fi

# Redirect output to the job log file.
exec > "$HTCONDOR_OUTPUT_FILE" 2>&1

# If PARALLEL_SERVER_ environment variables are not set, assign any
# available values with form MDCE_ for backwards compatibility
PARALLEL_SERVER_CMR=${PARALLEL_SERVER_CMR:="${MDCE_CMR}"}
PARALLEL_SERVER_MATLAB_EXE=${PARALLEL_SERVER_MATLAB_EXE:="${MDCE_MATLAB_EXE}"}
PARALLEL_SERVER_MATLAB_ARGS=${PARALLEL_SERVER_MATLAB_ARGS:="${MDCE_MATLAB_ARGS}"}
PARALLEL_SERVER_TOTAL_TASKS=${PARALLEL_SERVER_TOTAL_TASKS:="${MDCE_TOTAL_TASKS}"}
PARALLEL_SERVER_NUM_THREADS=${PARALLEL_SERVER_NUM_THREADS:="${MDCE_NUM_THREADS}"}
PARALLEL_SERVER_DEBUG=${PARALLEL_SERVER_DEBUG:="${MDCE_DEBUG}"}

# Condor will set TMPDIR to a folder it will create under its EXECUTE folder,
# but on slow filesystems we might try to use the folder before we see it's
# been created. Set TMPDIR back to /tmp here to avoid this.
export TMPDIR=/tmp
PARALLEL_SERVER_GENVLIST="${PARALLEL_SERVER_GENVLIST},TMPDIR"

# Other environment variables to forward
PARALLEL_SERVER_GENVLIST="${PARALLEL_SERVER_GENVLIST},HOME,USER"

# Remove the contact file, so if we are held and released
# it can be recreated anew
rm -f $CONDOR_CONTACT_FILE

# Add libexec to path
PATH=`condor_config_val libexec`/:$PATH

echo "Executing MPIEXEC"
# SLOTS is a list of the slots assigned by HTCondor to the MPI job
SLOTS=$($(condor_config_val libexec)/condor_chirp get_job_attr AllRemoteHosts)
# MACHINE_FILE will contain a list of server names to provide to mpiexec via the machinefile argument
MACHINE_FILE="${_CONDOR_SCRATCH_DIR}/hosts"
echo $SLOTS |  sed -e 's/\"\(.*\)\".*/\1/' -e 's/,/\n/g' |tr  "@" "\n"| grep -v slot | sort >> ${MACHINE_FILE}

# Create full path to mw_mpiexec if needed.
FULL_MPIEXEC=${PARALLEL_SERVER_CMR:+${PARALLEL_SERVER_CMR}/bin/}mw_mpiexec

# Label stdout/stderr with the rank of the process
MPI_VERBOSE=-l

# Increase the verbosity of mpiexec if PARALLEL_SERVER_DEBUG is set and not false
if [ ! -z "${PARALLEL_SERVER_DEBUG}" ] && [ "${PARALLEL_SERVER_DEBUG}" != "false" ] ; then
    MPI_VERBOSE="${MPI_VERBOSE} -v -print-all-exitcodes"
fi

if [ ! -z "${PARALLEL_SERVER_BIND_TO_CORE}" ] && [ "${PARALLEL_SERVER_BIND_TO_CORE}" != "false" ] ; then
    BIND_TO_CORE_ARG="-bind-to core:${PARALLEL_SERVER_NUM_THREADS}"
else
    BIND_TO_CORE_ARG=""
fi

# Construct the command to run.
CMD="\"${FULL_MPIEXEC}\" \
    ${PARALLEL_SERVER_MPIEXEC_ARG} \
    -genvlist ${PARALLEL_SERVER_GENVLIST} \
    ${BIND_TO_CORE_ARG} \
    ${MPI_VERBOSE} \
    -machinefile ${MACHINE_FILE} \
    -wdir ${TMPDIR} \
    -n ${PARALLEL_SERVER_TOTAL_TASKS} \
    \"${PARALLEL_SERVER_MATLAB_EXE}\" \
    ${PARALLEL_SERVER_MATLAB_ARGS}"

# Echo the command so that it is shown in the output log.
echo $CMD

# Execute the command.
eval $CMD

MPIEXEC_EXIT_CODE=${?}
if [ ${MPIEXEC_EXIT_CODE} -eq 42 ] ; then
    # Get here if user code errored out within MATLAB. Overwrite this to zero in
    # this case.
    echo "Overwriting MPIEXEC exit code from 42 to zero (42 indicates a user-code failure)"
    MPIEXEC_EXIT_CODE=0
fi
echo "Exiting with code: ${MPIEXEC_EXIT_CODE}"
exit ${MPIEXEC_EXIT_CODE}
