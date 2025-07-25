// Copyright 2023 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

import userEvent from '@testing-library/user-event';
import * as bhSharedUI from 'bh-shared-ui';
import { render } from 'src/test-utils';
import CopyMenuItem from './CopyMenuItem';

const useExploreSelectedItemSpy = vi.spyOn(bhSharedUI, 'useExploreSelectedItem');

describe('CopyMenuItem', () => {
    const selectedNode = {
        label: 'foo',
    };

    const setup = () => {
        useExploreSelectedItemSpy.mockReturnValue({ selectedItemQuery: { data: selectedNode } } as any);
        const screen = render(<CopyMenuItem />);
        return screen;
    };

    it('handles copying the name', async () => {
        const screen = setup();

        const user = userEvent.setup();

        const copyOption = screen.getByRole('menuitem', { name: /copy/i });
        await user.hover(copyOption);

        const tooltip = await screen.findByRole('tooltip');
        expect(tooltip).toBeInTheDocument();

        // the tooltip container and the menu item for `name` have the same accessible name, so return the second element here (which is the menu item)
        const nameOption = screen.getAllByRole('menuitem', { name: /name/i })[1];
        await user.click(nameOption);

        const clipboardText = await navigator.clipboard.readText();
        expect(clipboardText).toBe(selectedNode.label);
    });
});
