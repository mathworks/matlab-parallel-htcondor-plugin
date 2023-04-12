function createCommunicatingSubmitDescriptionFile(outputFilename, logFile, condorLogFile, wrapperScriptName, ...
    numberOfTasks, additionalSubmitArgs, jobDirectory)
% Create a submit description file to be used with the
% HTCondor condor_submit command for communicating jobs.

% Copyright 2020-2023 The MathWorks, Inc.

% Split additionalSubmitArgs into separate lines
args = split(additionalSubmitArgs, ' ');

% Open file in binary mode to make it cross-platform.
fid = fopen(outputFilename, 'w');
if fid < 0
    error('parallelexamples:GenericHTCondor:FileError', ...
        'Failed to open file %s for writing', outputFilename);
end
cleanup = onCleanup(@() fclose(fid));

environmentString = formatEnvironmentString({'MPIEXEC_WORKING_DIR', jobDirectory});

fprintf(fid, 'executable=%s\n', wrapperScriptName);
fprintf(fid, 'Universe=parallel\n');
fprintf(fid, 'Initialdir=%s\n', jobDirectory);
fprintf(fid, 'output=%s\n', logFile);
fprintf(fid, 'error=%s\n', logFile);
fprintf(fid, 'log=%s\n', condorLogFile);
fprintf(fid, 'environment=%s\n', environmentString);
fprintf(fid, 'machine_count=%d\n', numberOfTasks);
fprintf(fid, '%s\n', args{:});
fprintf(fid, 'Queue\n');

end
