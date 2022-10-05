function createSubmitDescriptionFile(outputFilename, logFile, condorLogFile, wrapperScriptName, ...
    jobArraySize, additionalSubmitArgs, taskOffset, jobDirectory, environmentVariables)
% Create a submit description file to be used with the
% HTCondor condor_submit command for independent jobs.

% Copyright 2020-2022 The MathWorks, Inc.

% Split additionalSubmitArgs into separate lines
args = split(additionalSubmitArgs, ' ');

% Open file in binary mode to make it cross-platform.
fid = fopen(outputFilename, 'w');
if fid < 0
    error('parallelexamples:GenericHTCondor:FileError', ...
        'Failed to open file %s for writing', outputFilename);
end
cleanup = onCleanup(@() fclose(fid));

if jobArraySize+taskOffset > 1
    environmentVariables = [environmentVariables; {'HTCONDOR_ARRAY_TASK_ID', '$(Process)+1'}];
    queueCmd = sprintf('Queue %d\n', jobArraySize);
else
    queueCmd = 'Queue\n';
end
environmentString = formatEnvironmentString(environmentVariables);

fprintf(fid, 'executable=%s\n', wrapperScriptName);
fprintf(fid, 'Universe=vanilla\n');
fprintf(fid, 'Initialdir=%s\n', jobDirectory);
fprintf(fid, 'output=%s\n', logFile);
fprintf(fid, 'error=%s\n', logFile);
fprintf(fid, 'log=%s\n', condorLogFile);
fprintf(fid, 'environment=%s\n', environmentString);
fprintf(fid, 'JOB_ID=$$([$(Process)+%d])\n', taskOffset+1);
fprintf(fid, '%s\n', args{:});
fprintf(fid, queueCmd);

end
