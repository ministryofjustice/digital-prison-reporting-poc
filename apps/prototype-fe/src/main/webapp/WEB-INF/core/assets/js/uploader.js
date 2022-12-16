function createUploader(zone, get_sig_url) {
    var uploader = new Dropzone(zone, {
        url: $(zone).attr('action'),
        method: "post",
        autoProcessQueue: true,
        maxfiles: 1,
        maxFilesize: 5,
        timeout: null,
        acceptedFiles: "text/csv,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        maxThumbnailFilesize: 8, // 3MB
        thumbnailWidth: 150,
        thumbnailHeight: 150,
        /**
         *
         * @param object   file https://developer.mozilla.org/en-US/docs/Web/API/File
         * @param function done Use done('my error string') to return an error
         */
         
        /*
        accept: function(file, done) {
            file.postData = [];
            $.ajax({
                url: get_sig_url,
                data: {
                    filename: file.name
                },
                type: 'POST',
                dataType: 'json',
                success: function(response) {
                    if (!response.success)
                        done(response.message);

                    delete response.success;
                    file.custom_status = 'ready';
                    file.postData = response;
                    file.s3 = response.key;
                    $(file.previewTemplate).addClass('uploading');
                    done();
                },

                error: function(response) {
                    file.custom_status = 'rejected';
                    if (response.responseText) {
                        response = JSON.parse(response.responseText);
                    }
                    if (response.message) {
                        done(response.message);
                        return;
                    }
                    done('error preparing the upload');
                }
            });
        },
        * END ACCEPT */
        
        /**
         * Called just before each file is sent.
         * @param object   file https://developer.mozilla.org/en-US/docs/Web/API/File
         * @param object   xhr
         * @param object   formData https://developer.mozilla.org/en-US/docs/Web/API/FormData
         */
        /*
        sending: function(file, xhr, formData) {
            $.each(file.postData, function(k, v) {
                formData.append(k, v);
            });
            formData.append('Content-type', 'application/octet-stream');
            // formData.append('Content-length', '');
            formData.append('acl', 'public-read');
            formData.append('success_action_status', '200');
        }
        * END SENDING */
    });
  return uploader;
}