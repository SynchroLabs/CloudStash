// This module is to manage the Dropbox error information and provide helper methods to generate
// Dropbox errors.
//
// cant_copy_shared_folder
// cant_nest_shared_folder
// cant_move_folder_into_itself
// too_many_files
// duplicated_or_nested_paths
// cant_transfer_ownership
//
// path (LookupError) - list_folder / search
//   malformed_path
//   not_found
//   not_file
//   not_folder
//   restricted_content 
//
// from_lookup (LookupError) - copy/move
//   malformed_path
//   not_found
//   not_file
//   not_folder
//   restricted_content 
//
// from_write (WriteError) - copy/move
//   malformed_path
//   conflict
//   no_write_permission
//   insufficient_space
//   disallowed_name
//   team_folder
//
// to (WriteError) - copy/move
//   malformed_path
//   conflict
//   no_write_permission
//   insufficient_space
//   disallowed_name
//   team_folder
//
// path_lookup (LookupError) - delete
//   malformed_path
//   not_found
//   not_file
//   not_folder
//   restricted_content
//
// path_write (WriteError) - delete
//   malformed_path
//   conflict
//   no_write_permission
//   insufficient_space
//   disallowed_name
//   team_folder
//
// path (UploadWriteFailed) - upload
//   reason (WriteError)
//     malformed_path
//     conflict
//     no_write_permission
//     insufficient_space
//     disallowed_name
//     team_folder
//   upload_session_id
//
// path (LookupError) - list_folder/continue
//   malformed_path
//   not_found
//   not_file
//   not_folder
//   restricted_content
// reset (union with path above)
//
// PollError - copy_batch/check, delete_batch/check, move_batch/check, save_url/check_job_status, upload_session/finish_batch/check
//
// invalid_async_job_id
// internal_error
//
// other
//

getDropboxError = function(tag, subTag, message)
{
    var err = 
    { 
        "error_summary": tag + "/" + (subTag || "..."), 
        "error": { ".tag": tag } 
    };

    if (subTag)
    {
        err.error[tag] = { ".tag": subTag };
    }

    // user_message - An optional field. If present, it includes a message that can be shown directly to the end
    //                user of your app. You should show this message if your app is unprepared to programmatically
    //                handle the error returned by an endpoint.
    if (message)
    {
        err.user_message = message;
    }

    return err;
}

// The idea is that you can generate and throw/return a DropboxError anywhere in your code, and when it
// gets back to the top level request processor and you call returnDropboxError, the contained Dropbox error
// will be sent as the response.
//
function DropboxError(tag, subtag, message) 
{
    Error.captureStackTrace(this, this.constructor);
    this.name = 'DropboxError';
    this.message = message || 'Dropbox Error';
    this.dropboxError = getDropboxError(tag, subtag, message);
}
DropboxError.prototype = Object.create(Error.prototype);
DropboxError.prototype.constructor = DropboxError;

exports.DropboxError = DropboxError;

exports.getDropboxError = function(err)
{
    return err.dropboxError || getDropboxError("other", null, err.message);
}

exports.returnDropboxError = function(res, err)
{
    // If the error is a DropboxError, we'll send the Dropbox response.  If not, we'll send an
    // "other" response with the message from the error.
    //
    res.send(409, err.dropboxError || getDropboxError("other", null, err.message));
}

exports.returnDropboxErrorNew = function(res, tag, subtag, message)
{
    res.send(409, getDropboxError(tag, subtag, message));
}

exports.returnDropboxErrorOther = function(res, message)
{
    res.send(409, getDropboxError("other", null, message));
}
