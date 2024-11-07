// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//
// Copyright 2024 FeatureForm Inc.
//

import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import FilterPanel from './FilterPanel'; // Adjust the path if necessary

describe('FilterPanel Component', () => {
  const mockOnCheckBoxChange = jest.fn();

  const defaultFilters = {
    ProviderType: [],
    Status: [],
  };

  it('renders provider type filter with checkboxes', () => {
    render(
      <FilterPanel
        filters={defaultFilters}
        onCheckBoxChange={mockOnCheckBoxChange}
      />
    );

    // Check if provider type labels are rendered
    expect(screen.getByText('Provider Type')).toBeInTheDocument();
    expect(screen.getByLabelText('Online')).toBeInTheDocument();
    expect(screen.getByLabelText('Offline')).toBeInTheDocument();
    expect(screen.getByLabelText('File')).toBeInTheDocument();
  });

  it('renders status filter with checkboxes', () => {
    render(
      <FilterPanel
        filters={defaultFilters}
        onCheckBoxChange={mockOnCheckBoxChange}
      />
    );

    // Check if status labels are rendered
    expect(screen.getByText('Status')).toBeInTheDocument();
    expect(screen.getByLabelText('Connected')).toBeInTheDocument();
    expect(screen.getByLabelText('Disconnected')).toBeInTheDocument();
  });

  it('calls onCheckBoxChange when a checkbox is clicked', () => {
    render(
      <FilterPanel
        filters={defaultFilters}
        onCheckBoxChange={mockOnCheckBoxChange}
      />
    );

    // Simulate checking the 'Online' checkbox
    const onlineCheckbox = screen.getByLabelText('Online');
    fireEvent.click(onlineCheckbox);

    expect(mockOnCheckBoxChange).toHaveBeenCalledWith('ProviderType', 'online');
  });

  it('handles expanded and collapsed accordions', () => {
    const { getByText } = render(
      <FilterPanel
        filters={defaultFilters}
        onCheckBoxChange={mockOnCheckBoxChange}
      />
    );

    const providerTypeLabel = getByText('Provider Type');
    fireEvent.click(providerTypeLabel); // Collapse provider type accordion

    expect(screen.getByText('Provider Type')).toBeInTheDocument();
  });
});
