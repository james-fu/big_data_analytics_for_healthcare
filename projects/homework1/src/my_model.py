import utils
import pandas as pd
#Note: You can reuse code that you wrote in etl.py and models.py and cross.py over here. It might help.
# PLEASE USE THE GIVEN FUNCTION NAME, DO NOT CHANGE IT

'''
You may generate your own features over here.
Note that for the test data, all events are already filtered such that they fall in the observation window of their respective patients. Thus, if you were to generate features similar to those you constructed in code/etl.py for the test data, all you have to do is aggregate events for each patient.
IMPORTANT: Store your test data features in a file called "test_features.txt" where each line has the
patient_id followed by a space and the corresponding feature in sparse format.
Eg of a line:
60 971:1.000000 988:1.000000 1648:1.000000 1717:1.000000 2798:0.364078 3005:0.367953 3049:0.013514
Here, 60 is the patient id and 971:1.000000 988:1.000000 1648:1.000000 1717:1.000000 2798:0.364078 3005:0.367953 3049:0.013514 is the feature for the patient with id 60.

Save the file as "test_features.txt" and save it inside the folder deliverables

input:
output: X_train,Y_train,X_test
'''
def my_features():
    events_df, mortality_df, feature_map_df = utils.read_csv('../data/train/')

    print events_df.event_id.unique().shape


    def agg_events(df):
        event_name = df.event_id.iloc[0]

        if 'LAB' in event_name:
            return df.patient_id.count()

        elif 'DIAG' in event_name or 'DRUG' in event_name:
            return df.value.sum()

    aggregated_events = events_df.groupby(['patient_id',
                                           'event_id']).apply(agg_events)

    aggregated_events.name = 'value'
    aggregated_events = aggregated_events.reset_index()
    pivoted = aggregated_events.pivot(index='patient_id', columns='event_id', values='value')
    pivoted = pivoted / pivoted.max()
    print pd.melt(pivoted).dropna()
    return None,None,None


'''
You can use any model you wish.

input: X_train, Y_train, X_test
output: Y_pred
'''
def my_classifier_predictions(X_train,Y_train,X_test):
    #TODO: complete this
    return None


def main():
    X_train, Y_train, X_test = my_features()
    Y_pred = my_classifier_predictions(X_train,Y_train,X_test)
    # utils.generate_submission("../deliverables/test_features.txt",Y_pred)
    #The above function will generate a csv file of (patient_id,predicted label) and will be saved as "my_predictions.csv" in the deliverables folder.

if __name__ == "__main__":
    main()


