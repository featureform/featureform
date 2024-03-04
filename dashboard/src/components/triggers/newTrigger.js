import CloseIcon from '@mui/icons-material/Close';
import { Box, Button, IconButton, TextField } from '@mui/material';
import React, { useState } from 'react';
import { useDataAPI } from '../../hooks/dataAPI';
const cron = require('cron-validator');

export default function NewTrigger({ handleClose }) {
  const initialState = Object.freeze({
    triggerName: '',
    schedule: '',
  });
  const [errorBag, setErrorBag] = useState({});
  const [formValues, setFormValues] = useState(initialState);
  const dataAPI = useDataAPI();

  const handleChange = (e) => {
    const { name, value } = e.target;
    let trimValue = '';

    if (value?.trim()) {
      trimValue = value;
    }

    setFormValues({
      ...formValues,
      [name]: trimValue,
    });
  };

  const clearForm = () => {
    setFormValues({ ...initialState });
    setErrorBag({});
    handleClose?.();
  };

  const validateForm = () => {
    const errorObj = {};
    const inputs = ['triggerName', 'schedule'];

    inputs.forEach((inputName) => {
      if (!formValues[inputName]) {
        errorObj[inputName] = 'Enter a value.';
      }
    });

    //check cron validation
    if (formValues['schedule']) {
      const isCronValid = cron.isValidCron(formValues['schedule'], {
        alias: true,
      });
      if (!isCronValid) {
        errorObj['schedule'] = 'Cron expression is invalid.';
      }
    }

    setErrorBag(errorObj);
    return !Object.keys(errorObj).length;
  };

  const submitForm = (e) => {
    e.preventDefault();
    const isValid = validateForm();
    if (isValid) {
      dataAPI.postTrigger(formValues);
      handleClose?.();
    }
  };

  return (
    <>
      <Box sx={{ margin: '1.25em' }}>
        <Box sx={{ marginBottom: '2em' }}>
          <strong>Create New Trigger</strong>
          <IconButton
            size='large'
            onClick={() => handleClose?.()}
            sx={{ position: 'absolute', right: '10px', top: '5px' }}
          >
            <CloseIcon />
          </IconButton>
        </Box>
        <TextField
          variant='outlined'
          error={!!errorBag?.triggerName}
          helperText={errorBag.triggerName || ''}
          required
          margin='normal'
          InputLabelProps={{ shrink: true }}
          fullWidth
          label='Trigger Name'
          name='triggerName'
          value={formValues.triggerName}
          onChange={handleChange}
          inputProps={{
            'aria-label': 'trigger name input',
            'data-testid': 'triggerNameInputId',
          }}
        />

        <TextField
          variant='outlined'
          error={!!errorBag?.schedule}
          helperText={errorBag.schedule || ''}
          required
          margin='normal'
          InputLabelProps={{ shrink: true }}
          fullWidth
          label='Schedule'
          name='schedule'
          value={formValues.schedule}
          multiline
          rows={2}
          onChange={handleChange}
          inputProps={{
            'aria-label': 'trigger schedule input',
            'data-testid': 'triggerScheduleInputId',
          }}
        />

        <Box sx={{ marginTop: '1em' }} display={'flex'} justifyContent={'end'}>
          <Button
            variant='contained'
            onClick={clearForm}
            sx={{ margin: '0.5em', background: '#FFFFFF', color: '#000000' }}
          >
            Cancel
          </Button>
          <Button
            variant='contained'
            onClick={submitForm}
            sx={{ margin: '0.5em', background: '#7A14E5' }}
            data-testid='saveNewTriggerId'
          >
            Save
          </Button>
        </Box>
      </Box>
    </>
  );
}
