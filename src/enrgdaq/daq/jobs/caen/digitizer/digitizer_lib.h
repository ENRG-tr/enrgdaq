
typedef void (*python_callback_t)(CAEN_DGTZ_EventInfo_t *event_info, CAEN_DGTZ_UINT16_EVENT_t *event_data);

void run_acquisition(int handle, python_callback_t callback);
void stop_acquisition();
