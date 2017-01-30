import utils
import etl
import pandas as pd
import numpy as np
import time
from sklearn.pipeline import Pipeline
from sklearn.decomposition import PCA, TruncatedSVD
from sklearn.ensemble import RandomForestClassifier
from sklearn.cross_validation import StratifiedKFold, ShuffleSplit
from sklearn.grid_search import GridSearchCV
from sklearn.externals import joblib
from sklearn.tree import DecisionTreeClassifier, DecisionTreeRegressor
from sklearn.linear_model import LassoCV, LogisticRegressionCV
from sklearn.ensemble import GradientBoostingClassifier, AdaBoostClassifier
from sklearn.svm import LinearSVC, SVC
from sklearn.manifold import TSNE
from sklearn.feature_extraction.text import CountVectorizer
from sklearn import feature_selection
from sklearn import preprocessing

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

    all_features = set(feature_map.idx.unique())

    test_events, _, _ = utils.read_csv('../data/test/')

    train_events = clean_training_data(train_events.iloc[:, :], train_mortality)

    train_features, test_features = extra_features(train_events, test_events)

    patient_id_series = pd.Series(train_features.index,
                                  index=train_features.index)

    dead_ids = list(train_mortality.patient_id)
    train_labels = np.array([id in dead_ids for id in list(patient_id_series)])

    X_train = train_features
    Y_train = train_labels




    X_test = test_features.sort_index()


    test_features.index.name = 'patient_id'
    test_features_long = pd.melt(test_features.reset_index(),id_vars=['patient_id'] )
    test_features_long.columns = ['patient_id', 'feature_id', 'feature_value']
    test_features_long = test_features_long.sort_values('patient_id')

    save_test_features(test_features_long)
    print 'Feature creation took {} seconds!'.format(time.time()-start)

    return X_train, Y_train, X_test

def extra_features(train_events_df, test_events_df):
    train_descriptions = train_events_df.groupby('patient_id').event_description.apply(lambda x:
                                                                  x.str.cat(sep=' '))

    test_descriptions = test_events_df.groupby('patient_id').event_description.apply(lambda x:
                                                                  x.str.cat(sep=' '))


    counter = CountVectorizer(ngram_range=(1, 2), min_df=0.1, max_df=0.8)
    train_events = counter.fit_transform(train_descriptions)
    test_events = counter.transform(test_descriptions)


    return (pd.DataFrame(train_events.toarray(), index=train_descriptions.index),
            pd.DataFrame(test_events.toarray(), index=test_descriptions.index))

def clean_training_data(train_events, train_mortality):
    indx_dates = etl.calculate_index_date(train_events,
                                          train_mortality,
                                          '/tmp/')

    return etl.filter_events(train_events, indx_dates, '/tmp/')

def add_columns(full_set, df):
    to_add = full_set.difference(df.columns)

    new_df = pd.DataFrame(columns=to_add, index=df.index)

    full_df = pd.concat((df, new_df), axis=1)
    full_df = full_df.reindex_axis(sorted(full_df.columns), axis=1)

    return full_df.fillna(0)

def save_test_features(test_features_long):
    tuple_dict = test_features_long.groupby('patient_id').apply(lambda x:
                                                               list(x.sort_values('feature_id').apply(lambda y:
                                                                    (y.feature_id,
                                                                     y.feature_value),
                                                                     axis=1)))
    patient_features = tuple_dict.to_dict()

    deliverable1 = open('../deliverables/test_features.txt', 'wb')

    for patient in sorted(patient_features.keys()):
        deliverable1.write("{} {} \n".format(patient,
                                             utils.bag_to_svmlight(patient_features[patient])))
'''
You can use any model you wish.

input: X_train, Y_train, X_test
output: Y_pred
'''
def my_classifier_predictions(X_train,Y_train,X_test):

    clf1 = train_initial(X_train, Y_train)

    clf1_train_predictions = clf1.predict_proba(X_train)

    clf1_test_predictions = clf1.predict_proba(X_test)

    clf2 = train_secondary(np.concatenate((clf1_train_predictions, X_train),
                                          axis=-1), Y_train)

    utils.generate_kaggle_submission("../deliverables/test_features.txt",
                                     clf1.predict_proba(X_test)[:, 1])
    return clf2.predict(np.concatenate((clf1_test_predictions, X_test),
                                             axis=-1)).astype(int)


    utils.generate_kaggle_submission("../deliverables/test_features.txt",
                                     clf1.predict_proba(X_test)[:, 1])
    return clf1.predict(X_test).astype(int)

def train_initial(X_train, Y_train):

    clf = Pipeline(steps=[
                          ('red', preprocessing.MinMaxScaler()),
                          ('clf', RandomForestClassifier())])

    params = dict(
                  clf__n_estimators=np.arange(50, 151, 20),
                  clf__min_samples_split=np.arange(10, 151, 50),
                  clf__min_samples_leaf=np.arange(2, 11, 5)
                  )

    clf = GridSearchCV(clf, params, n_jobs=24, scoring='roc_auc',
                       verbose=0, cv=3)

    clf.fit(X_train, Y_train)
    best_clf = clf.best_estimator_
    print clf.best_score_
    print clf.best_params_

    # clf = LogisticRegressionCV(n_jobs=32, cv=cv, scoring='roc_auc')
    # clf.fit(X_train, Y_train)
    # best_clf = clf
    # print np.max(best_clf.scores_.values())

    # joblib.dump(best_clf, './best_model.pkl')


    return best_clf

def train_secondary(X_train, Y_train):

    clf = Pipeline(steps=[('pca',
                           feature_selection.SelectFromModel(DecisionTreeClassifier(),
                                                             threshold=0.05)),
                           ('dt', DecisionTreeClassifier())])

    params = dict(
                  dt__max_depth=np.arange(5, 105, 20),
                  )

    best_clf = GridSearchCV(clf, params, n_jobs=16, scoring='roc_auc',
                            verbose=0, cv=3)

    best_clf.fit(X_train, Y_train)

    joblib.dump(best_clf.best_estimator_, './best_secondary_model.pkl')

    print best_clf.best_score_
    print best_clf.best_params_

    return best_clf.best_estimator_


def main():
    X_train, Y_train, X_test = my_features()


    Y_pred = my_classifier_predictions(X_train,Y_train,X_test)
    # print Y_pred.shape
    utils.generate_submission("../deliverables/test_features.txt",Y_pred)
    #The above function will generate a csv file of (patient_id,predicted label) and will be saved as "my_predictions.csv" in the deliverables folder.

if __name__ == "__main__":
    main()


