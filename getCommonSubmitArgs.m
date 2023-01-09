function commonSubmitArgs = getCommonSubmitArgs(cluster)
% Get any additional submit arguments for the HTCondor condor_submit command
% that are common to both independent and communicating jobs.

% Copyright 2016-2022 The MathWorks, Inc.

commonSubmitArgs = '';
ap = cluster.AdditionalProperties;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% CUSTOMIZATION MAY BE REQUIRED %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% You may wish to support further cluster.AdditionalProperties fields here
% and modify the submission command arguments accordingly.

% Accounting group
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'AccountingGroup', 'char', 'accounting_group=%s');

% Accounting group user
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'AccountingGroupUsername', 'char', 'accounting_group_user=%s');

% Disk space required
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'RequestDisk', 'char', 'request_disk=%s');

% Memory required
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'RequestMemory', 'char', 'request_memory=%s');

% Priority
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'Priority', 'double', 'priority=%d');

% Requirements
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'Requirements', 'char', 'requirements=%s');

% Email notification
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'EmailAddress', 'char', 'notify_user=%s notification=Always');

% Catch all: directly append anything in the AdditionalSubmitArgs
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, ...
    'AdditionalSubmitArgs', 'char', '%s');

% Trim any whitespace
commonSubmitArgs = strtrim(commonSubmitArgs);

end

function commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, propName, propType, submitPattern)
arg = validatedPropValue(ap, propName, propType);
if ~isempty(arg) && (~islogical(arg) || arg)
    commonSubmitArgs = sprintf([commonSubmitArgs ' ' submitPattern], arg);
end
end
