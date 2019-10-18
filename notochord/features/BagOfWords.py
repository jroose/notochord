from .. import ABCArgumentGroup, App, WorkOrderArgs, export, schema, ABCCoherenceStore
import re

re_word = re.compile(r'[a-zA-Z]+')

__all__ = []

class BagOfWordsArgs(ABCArgumentGroup):
    def __call__(self, group):
        group.add_argument("--output-feature-set", type=unicode, action="store", metavar="NAME", default=None, help="Name of output feature set (required)")
        group.add_argument("--input-feature-set", type=unicode, action="store", metavar="NAME", default=None, help="Name of input feature set (required)")
        group.add_argument("--input-feature", type=unicode, action="store", metavar="NAME", default=None, help="Name of input feature")
        group.add_argument("--chunk-size", type=int, action="store", metavar="INT", default=None, help="Number or widgets per chunk")

@export
class BagOfWords(App):
    @staticmethod
    def build_parser_groups():
        return [BagOfWordsArgs(), WorkOrderArgs()] + App.build_parser_groups()

    def __init__(self, datadir, input_feature_set=None, output_feature_set=None, input_feature=None, min_idwidget=None, max_idwidget=None, widget_sets=None, chunk_size=None, **kwargs):
        super(BagOfWords, self).__init__(datadir, **kwargs)
        self.config['output_feature_set'] = output_feature_set or self.config['output_feature_set']
        self.config['input_feature_set'] = input_feature_set or self.config['input_feature_set']
        self.config['input_feature'] = input_feature or self.config.get('input_feature')
        self.config['widget_sets'] = widget_sets or self.config.get('widget_sets')
        self.config["chunk_size"] = chunk_size or self.config.get('chunk_size', 1024)
        self.config['min_idwidget'] = (min_idwidget, None)[min_idwidget is None]
        self.config['max_idwidget'] = (max_idwidget, None)[max_idwidget is None]

    def main(self):
        

if __name__ == "__main__":
    A = BagOfWords.from_args(sys.argv[1:])
    A.run()
