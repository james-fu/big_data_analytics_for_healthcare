from sklearn.datasets import load_svmlight_file
import utils
# load created svmlight file
# load validation svmlight file
# generate matrix that will get written to fill


def load_svm():
    my_training = load_svmlight_file('../deliverables/features_svmlight.train',
                                     n_features=3190)

    validation = load_svmlight_file('../data/features_svmlight.validate',
                                    n_features=3190)

    train_path = '../data/train/'
    events, mortality, feature_map = utils.read_csv(train_path)

    print len(events.dropna().patient_id.unique())
    print my_training[0].todense().shape, my_training[1].shape, my_training[1].sum()
    print validation[0].todense().shape, validation[1].shape, validation[1].sum()

if __name__ == '__main__':
    load_svm()

