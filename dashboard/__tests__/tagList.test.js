import Chip from '@material-ui/core/Chip';
import Adapter from '@wojtekmaj/enzyme-adapter-react-17';
import { configure, mount, shallow } from 'enzyme';
import 'jest-canvas-mock';
import React from 'react';
import { TagList } from '../src/components/resource-list/ResourceListView';

configure({ adapter: new Adapter() });

describe('TagList', () => {
  const exampleTags = ['a', 'b', 'c'];

  test('renders correctly with no tags', () => {
    const list = shallow(<TagList />);
    expect(list.children().length).toBe(0);
  });

  test('renders correctly with no active tags', () => {
    const list = shallow(<TagList tagClass='class-here' tags={exampleTags} />);
    expect(list).toMatchSnapshot();
  });

  test('highlights active tags', () => {
    const list = mount(
      <TagList activeTags={{ [exampleTags[1]]: true }} tags={exampleTags} />
    );
    const chips = list.find(Chip);
    expect(chips.at(0).prop('color')).toBe('default');
    expect(chips.at(1).prop('color')).toBe('secondary');
  });

  test('toggles tag on click', () => {
    const toggle = jest.fn();
    const list = mount(<TagList tags={exampleTags} toggleTag={toggle} />);
    list.find(Chip).at(0).simulate('click');
    expect(toggle).toHaveBeenCalledWith(exampleTags[0]);
  });
});
