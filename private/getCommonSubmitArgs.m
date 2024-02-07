function commonSubmitArgs = getCommonSubmitArgs(cluster)
% Get any additional submit arguments for the HTCondor condor_submit command
% that are common to both independent and communicating jobs.

% Copyright 2016-2023 The MathWorks, Inc.

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

function commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, propName, propType, submitPattern, defaultValue)
% Helper fcn to append a scheduler option to the submit string.
% Inputs:
%  commonSubmitArgs: submit string to append to
%  ap: AdditionalProperties object
%  propName: name of the property
%  propType: type of the property, i.e. char, double or logical
%  submitPattern: sprintf-style string specifying the format of the scheduler option
%  defaultValue (optional): value to use if the property is not specified in ap

if nargin < 6
    defaultValue = [];
end
arg = validatedPropValue(ap, propName, propType, defaultValue);
if ~isempty(arg) && (~islogical(arg) || arg)
    commonSubmitArgs = [commonSubmitArgs, ' ', sprintf(submitPattern, arg)];
end
end

function commonSubmitArgs = iAppendRequiredArgument(commonSubmitArgs, ap, propName, propType, submitPattern, errMsg) %#ok<DEFNU>
% Helper fcn to append a required scheduler option to the submit string.
% An error is thrown if the property is not specified in AdditionalProperties or is empty.
% Inputs:
%  commonSubmitArgs: submit string to append to
%  ap: AdditionalProperties object
%  propName: name of the property
%  propType: type of the property, i.e. char, double or logical
%  submitPattern: sprintf-style string specifying the format of the scheduler option
%  errMsg (optional): text to append to the error message if the property is not specified in ap

if ~isprop(ap, propName)
    errorText = sprintf('Required field %s is missing from AdditionalProperties.', propName);
    if nargin > 5
        errorText = [errorText newline errMsg];
    end
    error('parallelexamples:GenericHTCondor:MissingAdditionalProperties', errorText);
elseif isempty(ap.(propName))
    errorText = sprintf('Required field %s is empty in AdditionalProperties.', propName);
    if nargin > 5
        errorText = [errorText newline errMsg];
    end
    error('parallelexamples:GenericHTCondor:EmptyAdditionalProperties', errorText);
end
commonSubmitArgs = iAppendArgument(commonSubmitArgs, ap, propName, propType, submitPattern);
end
