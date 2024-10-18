function createSubmitScript(outputFilename, jobName, quotedWrapperPath)
% Create a script that runs the HTCondor condor_submit command.

% Copyright 2010-2024 The MathWorks, Inc.

dctSchedulerMessage(5, '%s: Creating submit script for %s at %s', mfilename, jobName, outputFilename);

% Open file in binary mode to make it cross-platform.
fid = fopen(outputFilename, 'w');
if fid < 0
    error('parallelexamples:GenericHTCondor:FileError', ...
        'Failed to open file %s for writing', outputFilename);
end
fileCloser = onCleanup(@() fclose(fid));

% Specify shell to use
fprintf(fid, '#!/bin/sh\n');

commandToRun = getSubmitString(quotedWrapperPath);
fprintf(fid, '%s\n', commandToRun);

end
