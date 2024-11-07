// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import '@testing-library/jest-dom/extend-expect';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import BaseFilterPanel from './BaseFilterPanel'; // Adjust path as needed

describe('BaseFilterPanel', () => {
  const mockOnTextFieldEnter = jest.fn();

  afterEach(() => {
    jest.clearAllMocks();
  });

  test('renders filter panel with the "Filters" title and text field', () => {
    render(<BaseFilterPanel onTextFieldEnter={mockOnTextFieldEnter} />);

    // Check if the title is rendered
    const filterHeader = screen.getByTestId('filter-heading');
    expect(filterHeader).toBeInTheDocument();
    expect(filterHeader).toHaveTextContent('Filters');

    // Check if the text field is rendered
    const textField = screen.getByLabelText(/name/i);
    expect(textField).toBeInTheDocument();
    expect(textField).toHaveValue(''); // Should initially be empty
  });

  test('updates the text field value on user input', () => {
    render(<BaseFilterPanel onTextFieldEnter={mockOnTextFieldEnter} />);

    // Simulate user typing into the text field
    const textField = screen.getByLabelText(/name/i);
    fireEvent.change(textField, { target: { value: 'Test Name' } });

    // Check if the text field value has changed
    expect(textField).toHaveValue('Test Name');
  });

  test('calls onTextFieldEnter when Enter key is pressed', () => {
    render(<BaseFilterPanel onTextFieldEnter={mockOnTextFieldEnter} />);

    // Simulate user typing into the text field
    const textField = screen.getByLabelText(/name/i);
    fireEvent.change(textField, { target: { value: 'Test Name' } });

    // Simulate pressing the Enter key
    fireEvent.keyDown(textField, { key: 'Enter', code: 'Enter' });

    // Check if onTextFieldEnter was called with the text field value
    expect(mockOnTextFieldEnter).toHaveBeenCalledWith('Test Name');
  });

  test('clears the text field and calls onTextFieldEnter with empty string when cleared', () => {
    render(<BaseFilterPanel onTextFieldEnter={mockOnTextFieldEnter} />);

    // Simulate user typing into the text field
    const textField = screen.getByLabelText(/name/i);
    fireEvent.change(textField, { target: { value: 'Test Name' } });

    // Clear the text field
    fireEvent.change(textField, { target: { value: '' } });

    // Check if the text field value is cleared
    expect(textField).toHaveValue('');

    // Check if onTextFieldEnter was called with an empty string
    expect(mockOnTextFieldEnter).toHaveBeenCalledWith('');
  });

  test('renders children components inside BaseFilterPanel', () => {
    render(
      <BaseFilterPanel>
        <div data-testid='child-component'>Child Component</div>
      </BaseFilterPanel>
    );

    // Check if the child component is rendered
    const childComponent = screen.getByTestId('child-component');
    expect(childComponent).toBeInTheDocument();
    expect(childComponent).toHaveTextContent('Child Component');
  });
});
