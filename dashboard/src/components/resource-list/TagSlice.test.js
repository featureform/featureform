import { default as reducer, toggleTag } from "./TagSlice.js";

describe("TagSlice ", () => {
  const type = "Feature";
  it("toggle on tag", () => {
    const tag = "abc";
    const payload = { type, tag };
    const action = toggleTag(payload);
    const newState = reducer(undefined, action);
    expect(newState).toMatchObject({ [type]: { [tag]: true } });
  });

  it("toggle off tag", () => {
    const tag = "abc";
    const payload = { type, tag };
    const action = toggleTag(payload);
    const toggleOn = reducer(undefined, action);
    const toggleOff = reducer(toggleOn, action);
    expect(toggleOff).toMatchObject({ [type]: {} });
  });
});
