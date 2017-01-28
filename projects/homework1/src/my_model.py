import utils
import etl
import pandas as pd
import numpy as np
import time
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
    start = time.time()

    train_events, train_mortality, feature_map = utils.read_csv('../data/train/')

    print train_mortality.patient_id.max()
    print train_mortality.label.sum()

    test_events, test_mortality, _ = utils.read_csv('../data/test/')

    train_events = clean_training_data(train_events, train_mortality)
    train_features_long = etl.aggregate_events(train_events,
                                               None,
                                               feature_map,
                                               '/tmp/')

    train_features_array = train_features_long.pivot(index='patient_id',
                                                     columns='feature_id',
                                                     values='feature_value')

    train_features = train_features_array.fillna(0)

    train_patients = set(train_features.index.tolist())
    print len(train_patients.intersection(set(train_mortality.patient_id.tolist())))

    patient_id_series = pd.Series(train_features.index,
                                  index=train_features.index)

    dead_ids = list(train_mortality.patient_id)
    train_labels = [id in dead_ids for id in list(patient_id_series)]

    print np.sum(train_labels)

    # print train_labels

    print 'Feature creation took {} seconds!'.format(time.time()-start)
    return None,None,None

def clean_training_data(train_events, train_mortality):
    indx_dates = etl.calculate_index_date(train_events,
                                          train_mortality,
                                          '/tmp/')

    return etl.filter_events(train_events, indx_dates, '/tmp/')

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


