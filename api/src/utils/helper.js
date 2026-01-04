const moment = require('moment');
function dateTimeValidator(start_time, end_time) {
  let startTimeVerify = moment(start_time, "YYYY-MM-DDTHH:mm", true);

  let endTimeVerify = moment(end_time, "YYYY-MM-DDTHH:mm", true);

  if (start_time !== undefined && end_time !== undefined) {
    if (startTimeVerify.isValid() && endTimeVerify.isValid()) {
      if (endTimeVerify.isAfter(startTimeVerify)) {
        return {
          status: true
        }
      } else {
        return {
          status: false,
          stderr: "startTime must be less than or equal to endTime."
        }
      }
    } else {
      return {
        status: false,
        stderr: "Invalid date-time format. Please use 'YYYY-MM-DDTHH:mm'."
      }
    }
  }
}

module.exports = {
  dateTimeValidator,
};