function createSubmitScript(outputFilename, jobName, quotedScriptName, environmentVariables)
% Create a script that sets the correct environment variables and then
% executes the HTCondor condor_submit command.

% Copyright 2010-2022 The MathWorks, Inc.

dctSchedulerMessage(5, '%s: Creating submit script for %s at %s', mfilename, jobName, outputFilename);

% Open file in binary mode to make it cross-platform.
fid = fopen(outputFilename, 'w');
if fid < 0
    error('parallelexamples:GenericHTCondor:FileError', ...
        'Failed to open file %s for writing', outputFilename);
end
fileCloser = onCleanup(@() fclose(fid));

% Specify Shell to use
fprintf(fid, '#!/bin/sh\n');

% Write the commands to set and export environment variables
for ii = 1:size(environmentVariables, 1)
    fprintf(fid, 'export %s=''%s''\n', environmentVariables{ii,1}, environmentVariables{ii,2});
end

% Generate the command to run and write it.
commandToRun = getSubmitString(quotedScriptName);
fprintf(fid, '%s\n', commandToRun);

end
