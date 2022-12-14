function independentSubmitFcn(cluster, job, environmentProperties)
%INDEPENDENTSUBMITFCN Submit a MATLAB job to a HTCondor cluster
%
% Set your cluster's PluginScriptsLocation to the parent folder of this
% function to run it when you submit an independent job.
%
% See also parallel.cluster.generic.independentDecodeFcn.

% Copyright 2010-2022 The MathWorks, Inc.

% Store the current filename for the errors, warnings and
% dctSchedulerMessages.
currFilename = mfilename;
if ~isa(cluster, 'parallel.Cluster')
    error('parallelexamples:GenericHTCondor:NotClusterObject', ...
        'The function %s is for use with clusters created using the parcluster command.', currFilename)
end

decodeFunction = 'parallel.cluster.generic.independentDecodeFcn';

if cluster.HasSharedFilesystem
    error('parallelexamples:GenericHTCondor:NotNonSharedFileSystem', ...
        'The function %s is for use with nonshared filesystems.', currFilename)
end

if ~strcmpi(cluster.OperatingSystem, 'unix')
    error('parallelexamples:GenericHTCondor:UnsupportedOS', ...
        'The function %s only supports clusters with unix OS.', currFilename)
end

remoteConnection = getRemoteConnection(cluster);
[useJobArrays, maxJobArraySize] = iGetJobArrayProps(cluster, remoteConnection);
% Store data for future reference
cluster.UserData.UseJobArrays = useJobArrays;
if useJobArrays
    cluster.UserData.MaxJobArraySize = maxJobArraySize;
end

% Determine the debug setting. Setting to true makes the MATLAB workers
% output additional logging. If EnableDebug is set in the cluster object's
% AdditionalProperties, that takes precedence. Otherwise, look for the
% PARALLEL_SERVER_DEBUG and MDCE_DEBUG environment variables in that order.
% If nothing is set, debug is false.
enableDebug = 'false';
if isprop(cluster.AdditionalProperties, 'EnableDebug')
    % Use AdditionalProperties.EnableDebug, if it is set
    enableDebug = char(string(cluster.AdditionalProperties.EnableDebug));
else
    % Otherwise check the environment variables set locally on the client
    environmentVariablesToCheck = {'PARALLEL_SERVER_DEBUG', 'MDCE_DEBUG'};
    for idx = 1:numel(environmentVariablesToCheck)
        debugValue = getenv(environmentVariablesToCheck{idx});
        if ~isempty(debugValue)
            enableDebug = debugValue;
            break
        end
    end
end

% Get the correct quote and file separator for the Cluster OS.
% This check is unnecessary in this file because we explicitly
% checked that the ClusterOsType is unix.  This code is an example
% of how to deal with clusters that can be unix or pc.
if strcmpi(cluster.OperatingSystem, 'unix')
    quote = '''';
    fileSeparator = '/';
else
    quote = '"';
    fileSeparator = '\';
end

% The job specific environment variables
% Remove leading and trailing whitespace from the MATLAB arguments
matlabArguments = strtrim(environmentProperties.MatlabArguments);
% Where on the remote filesystem to store job output
storageLocation = remoteConnection.JobStorageLocation;
% If the RemoteJobStorageLocation ends with a space, add a slash to ensure it is respected
if endsWith(storageLocation, ' ')
    storageLocation = [storageLocation, fileSeparator];
end
variables = {'PARALLEL_SERVER_DECODE_FUNCTION', decodeFunction; ...
    'PARALLEL_SERVER_STORAGE_CONSTRUCTOR', environmentProperties.StorageConstructor; ...
    'PARALLEL_SERVER_JOB_LOCATION', environmentProperties.JobLocation; ...
    'PARALLEL_SERVER_MATLAB_EXE', environmentProperties.MatlabExecutable; ...
    'PARALLEL_SERVER_MATLAB_ARGS', matlabArguments; ...
    'PARALLEL_SERVER_DEBUG', enableDebug; ...
    'MLM_WEB_LICENSE', environmentProperties.UseMathworksHostedLicensing; ...
    'MLM_WEB_USER_CRED', environmentProperties.UserToken; ...
    'MLM_WEB_ID', environmentProperties.LicenseWebID; ...
    'PARALLEL_SERVER_LICENSE_NUMBER', environmentProperties.LicenseNumber; ...
    'PARALLEL_SERVER_STORAGE_LOCATION', storageLocation};
% Environment variable names different prior to 19b
if verLessThan('matlab', '9.7')
    variables(:,1) = replace(variables(:,1), 'PARALLEL_SERVER_', 'MDCE_');
end
% Trim the environment variables of empty values.
nonEmptyValues = cellfun(@(x) ~isempty(strtrim(x)), variables(:,2));
variables = variables(nonEmptyValues, :);

% The local job directory
localJobDirectory = cluster.getJobFolder(job);
% How we refer to the job directory on the cluster
remoteJobDirectory = remoteConnection.getRemoteJobLocation(job.ID, cluster.OperatingSystem);

% The script name is independentJobWrapper.sh
scriptName = 'independentJobWrapper.sh';
% The wrapper script is in the same directory as this file
dirpart = fileparts(mfilename('fullpath'));
localScript = fullfile(dirpart, scriptName);
% Copy the local wrapper script to the job directory
copyfile(localScript, localJobDirectory);

% The command that will be executed on the remote host to run the job.
remoteScriptName = sprintf('%s%s%s', remoteJobDirectory, fileSeparator, scriptName);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% CUSTOMIZATION MAY BE REQUIRED %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
additionalSubmitArgs = '';
commonSubmitArgs = getCommonSubmitArgs(cluster);
additionalSubmitArgs = strtrim(sprintf('%s %s', additionalSubmitArgs, commonSubmitArgs));

% Only keep and submit tasks that are not cancelled. Cancelled tasks
% will have errors.
isPendingTask = cellfun(@isempty, get(job.Tasks, {'Error'}));
tasks = job.Tasks(isPendingTask);
taskIDs = cell2mat(get(tasks, {'ID'}));
numberOfTasks = numel(tasks);

% Only use job arrays when you can get enough use out of them.
if numberOfTasks < 2
    useJobArrays = false;
end

if useJobArrays
    taskIDGroupsForJobArrays = iCalculateTaskIDGroupsForJobArrays(taskIDs, maxJobArraySize);
    
    jobName = sprintf('Job%d',job.ID);
    numJobArrays = numel(taskIDGroupsForJobArrays);
    commandsToRun = cell(numJobArrays, 1);
    jobIDs = cell(numJobArrays, 1);
    for ii = 1:numJobArrays
        taskOffset = taskIDGroupsForJobArrays{ii}(1) - 1;
        
        environmentVariables = [variables; ...
            {'PARALLEL_SERVER_TASK_ID_OFFSET', num2str(taskOffset)}];
        
        %HTCondor will fill in JOB_ID when submitting
        logFileName = 'Task$(JOB_ID).log';
        % generate log file name for the HTCondor log
        condorLogFileName = sprintf('Job%d.condor.log',job.ID);
        % Choose a file for the output. Please note that currently,
        % JobStorageLocation refers to a directory on disk, but this may
        % change in the future.
        logFile = sprintf('%s%s%s', remoteJobDirectory, fileSeparator, logFileName);
        condorLogFile = sprintf('%s%s%s', remoteJobDirectory, fileSeparator, condorLogFileName);
        quotedLogFile = sprintf('%s%s%s', quote, logFile, quote);
        dctSchedulerMessage(5, '%s: Using %s as log file', currFilename, quotedLogFile);
        
        % Create a script to submit a HTCondor job - this
        % will be created in the job directory
        dctSchedulerMessage(5, '%s: Generating script for job array %i', currFilename, ii);
        numberOfTasksToUse = numel(taskIDGroupsForJobArrays{ii});
        commandsToRun{ii} = iGetCommandToRun(localJobDirectory, remoteJobDirectory, fileSeparator, quote, jobName, ...
            logFile, remoteScriptName, environmentVariables, numberOfTasksToUse, additionalSubmitArgs, condorLogFile, taskOffset);
    end
else
    % Do not use job arrays and submit each task individually.
    taskLocations = environmentProperties.TaskLocations(isPendingTask);
    jobIDs = cell(1, numberOfTasks);
    commandsToRun = cell(numberOfTasks, 1);
    % Loop over every task we have been asked to submit
    for ii = 1:numberOfTasks
        taskLocation = taskLocations{ii};
        % Add the task location to the environment variables
        if verLessThan('matlab', '9.7') % variable name changed in 19b
            environmentVariables = [variables; ...
                {'MDCE_TASK_LOCATION', taskLocation}];
        else
            environmentVariables = [variables; ...
                {'PARALLEL_SERVER_TASK_LOCATION', taskLocation}];
        end
        
        % Choose a file for the output. Please note that currently,
        % JobStorageLocation refers to a directory on disk, but this may
        % change in the future.
        logFile = sprintf('%s%s%s', remoteJobDirectory, fileSeparator, sprintf('Task%d.log', taskIDs(ii)));
        % generate log file name for the HTCondor log
        condorLogFileName = sprintf('Job%d.condor.log',job.ID);
        condorLogFile = sprintf('%s%s%s', remoteJobDirectory, fileSeparator, condorLogFileName);
        quotedLogFile = sprintf('%s%s%s', quote, logFile, quote);
        dctSchedulerMessage(5, '%s: Using %s as log file', currFilename, quotedLogFile);
        
        % Submit one task at a time
        jobName = sprintf('Job%d.%d', job.ID, taskIDs(ii));
        
        % Create a script to submit a HTCondor job - this will be created in
        % the job directory
        dctSchedulerMessage(5, '%s: Generating script for task %i', currFilename, ii);
        commandsToRun{ii} = iGetCommandToRun(localJobDirectory, remoteJobDirectory, fileSeparator, quote, jobName, ...
            logFile, remoteScriptName, environmentVariables, 1, additionalSubmitArgs, condorLogFile, 0);
    end
end

% Start the mirror to copy all the job files over to the cluster
dctSchedulerMessage(4, '%s: Starting mirror for job %d.', currFilename, job.ID);
remoteConnection.startMirrorForJob(job);

% Add execute permissions to shell scripts
remoteConnection.runCommand(sprintf( ...
    'chmod u+x %s%s*.sh', remoteJobDirectory, fileSeparator));

for ii=1:numel(commandsToRun)
    commandToRun = commandsToRun{ii};
    jobIDs{ii} = iSubmitJobUsingCommand(remoteConnection, job, commandToRun);
end

% Calculate the schedulerIDs
if useJobArrays
    % The scheduler ID of each task is a combination of the job ID and the
    % scheduler array index. cellfun pairs each job ID with its
    % corresponding scheduler array indices in schedulerJobArrayIndices and
    % returns the combination of both. For example, if jobIDs = {1,2} and
    % schedulerJobArrayIndices = {[1,2];[3,4]}, the schedulerID is given by
    % combining 1 with [1,2] and 2 with [3,4], in the canonical form of the
    % scheduler.
    schedulerIDs = cell(numel(jobIDs), 1);
    for ii = 1:numel(jobIDs)
        schedulerIDs{ii} = jobIDs{ii} + "." + (0:numel(taskIDGroupsForJobArrays{ii}) - 1)';
    end
    schedulerIDs = vertcat(schedulerIDs{:});
else
    % The scheduler ID of each task is the job ID.
    schedulerIDs = string(jobIDs);
end

% Store the scheduler ID for each task and the job cluster data
% Set the cluster host and remote job storage location on the job cluster data
jobData = struct('type', 'generic', ...
    'RemoteHost', remoteConnection.Hostname, ...
    'RemoteJobStorageLocation', remoteConnection.JobStorageLocation, ...
    'HasDoneLastMirror', false);
if verLessThan('matlab', '9.7') % schedulerID stored in job data
    jobData.ClusterJobIDs = schedulerIDs;
else % schedulerID on task since 19b
    set(tasks, 'SchedulerID', schedulerIDs);
end
cluster.setJobClusterData(job, jobData);

end

function [useJobArrays, maxJobArraySize] = iGetJobArrayProps(cluster, ~)
% Look for useJobArrays and maxJobArray size in the following order:
% 1.  Additional Properties
% 2.  User Data
% 3.  Query scheduler for MaxJobArraySize

useJobArrays = validatedPropValue(cluster.AdditionalProperties, 'UseJobArrays', 'logical');
if isempty(useJobArrays)
    if isfield(cluster.UserData, 'UseJobArrays')
        useJobArrays = cluster.UserData.UseJobArrays;
    else
        useJobArrays = true;
    end
end

if ~useJobArrays
    % Not using job arrays so don't need the max array size
    maxJobArraySize = 0;
    return
end

maxJobArraySize = validatedPropValue(cluster.AdditionalProperties, 'MaxJobArraySize', 'numeric');
if ~isempty(maxJobArraySize)
    if maxJobArraySize < 1
        error('parallelexamples:GenericHTCondor:IncorrectArguments', ...
            'MaxJobArraySize must be a positive integer');
    end
    return
end

if isfield(cluster.UserData,'MaxJobArraySize')
    maxJobArraySize = cluster.UserData.MaxJobArraySize;
    return
end

% HTCondor has no upper bound on job array size
useJobArrays = true;
maxJobArraySize = inf;
end

function commandToRun = iGetCommandToRun(localJobDirectory, remoteJobDirectory, fileSeparator, quote, jobName, ...
    logFile, wrapperScriptName, environmentVariables, numberOfTasks, additionalSubmitArgs, condorLogFile,taskOffset)

localScriptName = sprintf('%s.sh', tempname(localJobDirectory));
[~, scriptName,extension] = fileparts(localScriptName);
scriptName = [scriptName extension];
localSubmitDescriptionFileName = sprintf('%s.sub',tempname(localJobDirectory));
[~, submitDescriptionFileName,extension] = fileparts(localSubmitDescriptionFileName);
submitDescriptionFileName = [submitDescriptionFileName extension];
remoteScriptLocation = sprintf('%s%s%s%s%s', quote, remoteJobDirectory, fileSeparator, scriptName, quote);
remoteSubmitDescriptionFileLocation = sprintf('%s%s%s%s%s', quote, remoteJobDirectory, fileSeparator, submitDescriptionFileName, quote);
createSubmitScript(localScriptName, jobName, remoteSubmitDescriptionFileLocation, environmentVariables);
createSubmitDescriptionFile(localSubmitDescriptionFileName, logFile, condorLogFile, wrapperScriptName, ...
    numberOfTasks, additionalSubmitArgs, taskOffset, remoteJobDirectory, environmentVariables);

% Create the command to run on the remote host.
commandToRun = sprintf('sh %s', remoteScriptLocation);
end

function jobID = iSubmitJobUsingCommand(remoteConnection, job, commandToRun)
currFilename = mfilename;
% Ask the cluster to run the submission command.
dctSchedulerMessage(4, '%s: Submitting job %d using command:\n\t%s', currFilename, job.ID, commandToRun);
% Execute the command on the remote host.
[cmdFailed, cmdOut] = remoteConnection.runCommand(commandToRun);
if cmdFailed
    % Stop the mirroring if we failed to submit the job - this will also
    % remove the job files from the remote location
    % Only stop mirroring if we are actually mirroring
    if remoteConnection.isJobUsingConnection(job.ID)
        dctSchedulerMessage(5, '%s: Stopping the mirror for job %d.', currFilename, job.ID);
        try
            remoteConnection.stopMirrorForJob(job);
        catch err
            warning('parallelexamples:GenericHTCondor:FailedToStopMirrorForJob', ...
                'Failed to stop the file mirroring for job %d.\nReason: %s', ...
                job.ID, err.getReport);
        end
    end
    error('parallelexamples:GenericHTCondor:FailedToSubmitJob', ...
        'Failed to submit job to HTCondor using command:\n\t%s.\nReason: %s', ...
        commandToRun, cmdOut);
end

jobID = extractJobId(cmdOut);
if isempty(jobID)
    error('parallelexamples:GenericHTCondor:FailedToParseSubmissionOutput', ...
        'Failed to parse the job identifier from the submission output: "%s"', ...
        cmdOut);
end
end

function taskIDGroupsForJobArrays = iCalculateTaskIDGroupsForJobArrays(taskIDsToSubmit, maxJobArraySize)
% Calculates the groups of task IDs to be submitted as job arrays

% We can only put tasks with sequential IDs into the same job array
% (the taskIDs will not be sequential if any tasks have been cancelled or
% deleted). We also need to ensure that each job array is smaller than
% maxJobArraySize.  So we first identify the sequential task IDs, and then
% split them apart into chunks of maxJobArraySize.

% The end of a range of sequential task IDs can be identifed where
% diff(taskIDsToSubmit) > 1. We also know the last taskID to be the end of
% a range.
isEndOfSequentialTaskIDs = [diff(taskIDsToSubmit) > 1; true];
endOfSequentialTaskIDsIdx = find(isEndOfSequentialTaskIDs);

% The difference between indices give the number of tasks in each range.
numTasksInEachRange = [endOfSequentialTaskIDsIdx(1); diff(endOfSequentialTaskIDsIdx)];

% The number of tasks in each job array must be less than maxJobArraySize.
jobArraySizes = arrayfun(@(x) iCalculateJobArraySizes(x, maxJobArraySize), numTasksInEachRange, 'UniformOutput', false);
jobArraySizes = [jobArraySizes{:}];
taskIDGroupsForJobArrays = mat2cell(taskIDsToSubmit, jobArraySizes);
end

function jobArraySizes = iCalculateJobArraySizes(numTasks, maxJobArraySize)
if isinf(maxJobArraySize)
    numJobArrays = 1;
else
    numJobArrays = ceil(numTasks./maxJobArraySize);
end
jobArraySizes = repmat(maxJobArraySize, 1, numJobArrays);
remainder = mod(numTasks, maxJobArraySize);
if remainder > 0
    jobArraySizes(end) = remainder;
end
end
